from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
from typing import Any, Callable, Sequence
import uuid


DEFAULT_CONFIG_PATH = "/etc/ucloud-sandboxes/hibernation-quota-helper.json"
HELPER_VERSION = 1
MAX_JSON_BYTES = 1024 * 1024
MAX_QUOTA_MB = 16 * 1024 * 1024

_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_INCARNATION = re.compile(r"([A-Za-z0-9][A-Za-z0-9_.:-]{0,127})\.sandbox-([0-9]+)\Z")
_PROJECT_ID = re.compile(r"[1-9][0-9]{0,9}\Z")
_PROJECT_STAT = re.compile(r"^fsxattr\.projid = ([0-9]+)$", re.MULTILINE)
_PROJECT_INHERIT = re.compile(
    r"^fsxattr\.xflags = 0x[0-9a-f]+ \[[^]]*\bproj-inherit\b[^]]*]$",
    re.MULTILINE,
)
_PROJECT_REPORT = re.compile(
    r"^#([0-9]+)\s+([0-9]+)\s+([0-9]+)\s+([0-9]+)\s",
    re.MULTILINE,
)


class QuotaHelperError(RuntimeError):
    pass


@dataclass(frozen=True)
class QuotaHelperConfig:
    mount_root: Path
    quota_root: Path
    xfs_io: Path
    xfs_quota: Path
    findmnt: Path


CommandRunner = Callable[[Sequence[str]], str]


def render_hibernation_quota_helper_script(
    *,
    config_path: str = DEFAULT_CONFIG_PATH,
) -> str:
    if not config_path.startswith("/") or "\n" in config_path or "\r" in config_path:
        raise ValueError("quota helper config path must be absolute and single-line")
    source = Path(__file__).read_text(encoding="utf-8")
    assignment = f"DEFAULT_CONFIG_PATH = {config_path!r}"
    lines = source.splitlines()
    indexes = [
        index
        for index, line in enumerate(lines)
        if line.startswith("DEFAULT_CONFIG_PATH = ")
    ]
    if len(indexes) != 1:
        raise RuntimeError("quota helper config marker is missing or ambiguous")
    lines[indexes[0]] = assignment
    return "#!/usr/bin/python3\n" + "\n".join(lines) + "\n"


def _validate_safe_id(label: str, value: str) -> str:
    if not _SAFE_ID.fullmatch(value) or value in {".", ".."}:
        raise QuotaHelperError(
            f"{label} must be 1-128 safe ASCII identifier characters"
        )
    return value


def _validate_nonnegative_int(label: str, value: Any) -> int:
    if isinstance(value, bool):
        raise QuotaHelperError(f"{label} must be a non-negative integer")
    if isinstance(value, int):
        result = value
    elif (
        isinstance(value, str)
        and value.isascii()
        and value.isdecimal()
        and (value == "0" or not value.startswith("0"))
    ):
        result = int(value)
    else:
        raise QuotaHelperError(f"{label} must be a non-negative integer")
    if result < 0:
        raise QuotaHelperError(f"{label} must be a non-negative integer")
    return result


def _validate_positive_int(label: str, value: Any, *, maximum: int) -> int:
    result = _validate_nonnegative_int(label, value)
    if not 1 <= result <= maximum:
        raise QuotaHelperError(f"{label} must be in [1, {maximum}]")
    return result


def _validated_absolute_path(label: str, value: Any) -> Path:
    if not isinstance(value, str) or not value.startswith("/"):
        raise QuotaHelperError(f"{label} must be an absolute path")
    path = Path(value)
    if str(path) != os.path.normpath(value) or ".." in path.parts:
        raise QuotaHelperError(f"{label} must be normalized and cannot contain '..'")
    return path


def _check_real_directory(
    path: Path,
    label: str,
    *,
    require_root_ownership: bool,
) -> os.stat_result:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise QuotaHelperError(f"{label} does not exist") from exc
    if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise QuotaHelperError(f"{label} must be a real directory")
    if require_root_ownership and info.st_uid != 0:
        raise QuotaHelperError(f"{label} must be owned by root")
    if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise QuotaHelperError(f"{label} cannot be group/world writable")
    return info


def _check_executable(
    path: Path,
    label: str,
    *,
    require_root_ownership: bool,
) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise QuotaHelperError(f"{label} does not exist") from exc
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise QuotaHelperError(f"{label} must be a regular file")
    if not info.st_mode & stat.S_IXUSR:
        raise QuotaHelperError(f"{label} must be executable")
    if require_root_ownership and info.st_uid != 0:
        raise QuotaHelperError(f"{label} must be owned by root")
    if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise QuotaHelperError(f"{label} cannot be group/world writable")


def _check_no_symlink_components(path: Path) -> None:
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        try:
            info = current.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISLNK(info.st_mode):
            raise QuotaHelperError(f"path component is a symlink: {current}")


def load_config(
    path: str | Path = DEFAULT_CONFIG_PATH,
    *,
    require_root_ownership: bool = True,
) -> QuotaHelperConfig:
    config_path = Path(path)
    try:
        descriptor = os.open(
            config_path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as exc:
        raise QuotaHelperError("quota helper config cannot be opened safely") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise QuotaHelperError("quota helper config must be a regular file")
        if require_root_ownership and info.st_uid != 0:
            raise QuotaHelperError("quota helper config must be owned by root")
        if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise QuotaHelperError("quota helper config cannot be group/world writable")
        if info.st_size > MAX_JSON_BYTES:
            raise QuotaHelperError("quota helper config is too large")
        try:
            raw = os.read(descriptor, MAX_JSON_BYTES + 1)
        except OSError as exc:
            raise QuotaHelperError("quota helper config cannot be read") from exc
    finally:
        os.close(descriptor)
    if len(raw) > MAX_JSON_BYTES:
        raise QuotaHelperError("quota helper config is too large")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QuotaHelperError("quota helper config is invalid JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "version",
        "mount_root",
        "quota_root",
        "xfs_io",
        "xfs_quota",
        "findmnt",
    }:
        raise QuotaHelperError("quota helper config has an invalid schema")
    if payload["version"] != HELPER_VERSION or isinstance(payload["version"], bool):
        raise QuotaHelperError("quota helper config has an unsupported version")
    mount_root = _validated_absolute_path("mount root", payload["mount_root"])
    quota_root = _validated_absolute_path("quota root", payload["quota_root"])
    if quota_root.parent != mount_root or quota_root.name != "ucloud-hibernation":
        raise QuotaHelperError("quota root must be mount_root/ucloud-hibernation")
    xfs_io = _validated_absolute_path("xfs_io", payload["xfs_io"])
    xfs_quota = _validated_absolute_path("xfs_quota", payload["xfs_quota"])
    findmnt = _validated_absolute_path("findmnt", payload["findmnt"])
    for candidate in (mount_root, quota_root):
        _check_no_symlink_components(candidate)
        _check_real_directory(
            candidate,
            candidate.name or "mount root",
            require_root_ownership=require_root_ownership,
        )
    for executable, label in (
        (xfs_io, "xfs_io"),
        (xfs_quota, "xfs_quota"),
        (findmnt, "findmnt"),
    ):
        _check_executable(
            executable,
            label,
            require_root_ownership=require_root_ownership,
        )
    return QuotaHelperConfig(
        mount_root=mount_root,
        quota_root=quota_root,
        xfs_io=xfs_io,
        xfs_quota=xfs_quota,
        findmnt=findmnt,
    )


def _subprocess_runner(command: Sequence[str]) -> str:
    result = subprocess.run(
        list(command),
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise QuotaHelperError(
            f"quota command failed ({result.returncode}): "
            f"{Path(command[0]).name}: {result.stderr.strip()}"
        )
    return result.stdout


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class XfsHibernationQuotaHelper:
    LOCK_NAME = ".quota.lock"
    SHARED_ROOT_LOCK_NAMES = frozenset({LOCK_NAME, ".store.lock"})

    def __init__(
        self,
        config: QuotaHelperConfig,
        *,
        runner: CommandRunner | None = None,
        require_root_ownership: bool = True,
    ) -> None:
        self.config = config
        self.runner = runner or _subprocess_runner
        self.require_root_ownership = require_root_ownership

    def prepare(
        self,
        sandbox_id: str,
        sandbox_generation: Any,
        project_id: Any,
        quota_mb: Any,
    ) -> dict[str, Any]:
        sandbox_id = _validate_safe_id("sandbox id", sandbox_id)
        generation = _validate_nonnegative_int("sandbox generation", sandbox_generation)
        project = _validate_positive_int("project id", project_id, maximum=(2**32) - 1)
        quota = _validate_positive_int("quota_mb", quota_mb, maximum=MAX_QUOTA_MB)
        with self._lock():
            self._require_xfs_project_quota()
            path = self._incarnation_path(sandbox_id, generation)
            if not os.path.lexists(path):
                path.mkdir(mode=0o700)
                _fsync_directory(self.config.quota_root)
            _check_real_directory(
                path,
                "sandbox quota directory",
                require_root_ownership=self.require_root_ownership,
            )
            current_project, _current_inherit = self._project_attributes(path)
            if current_project not in {0, project}:
                raise QuotaHelperError(
                    "sandbox quota directory belongs to another XFS project"
                )
            if current_project == 0 and any(path.iterdir()):
                raise QuotaHelperError(
                    "unassigned sandbox quota directory must be empty"
                )
            if current_project == project:
                existing_limit = self._hard_limits_mb().get(project, 0)
                if existing_limit not in {0, quota}:
                    raise QuotaHelperError(
                        "existing XFS project hard limit differs from replay"
                    )
            self.runner(
                (
                    str(self.config.xfs_io),
                    "-x",
                    "-c",
                    f"chproj -R {project}",
                    str(path),
                )
            )
            self.runner(
                (
                    str(self.config.xfs_io),
                    "-x",
                    "-c",
                    "chattr +P",
                    str(path),
                )
            )
            self.runner(
                (
                    str(self.config.xfs_quota),
                    "-x",
                    "-c",
                    f"limit -p bsoft={quota}m bhard={quota}m {project}",
                    str(self.config.mount_root),
                )
            )
            return self._inspect_path(
                sandbox_id=sandbox_id,
                generation=generation,
                path=path,
                expected_project=project,
                expected_quota_mb=quota,
            )

    def inspect(
        self,
        sandbox_id: str,
        sandbox_generation: Any,
    ) -> dict[str, Any]:
        sandbox_id = _validate_safe_id("sandbox id", sandbox_id)
        generation = _validate_nonnegative_int("sandbox generation", sandbox_generation)
        with self._lock():
            self._require_xfs_project_quota()
            path = self._incarnation_path(sandbox_id, generation)
            _check_real_directory(
                path,
                "sandbox quota directory",
                require_root_ownership=self.require_root_ownership,
            )
            project, _inherits = self._project_attributes(path)
            if project == 0:
                raise QuotaHelperError(
                    "sandbox quota directory has no XFS project assignment"
                )
            return self._inspect_path(
                sandbox_id=sandbox_id,
                generation=generation,
                path=path,
                expected_project=project,
                expected_quota_mb=None,
            )

    def drop(
        self,
        sandbox_id: str,
        sandbox_generation: Any,
        project_id: Any,
    ) -> dict[str, Any]:
        sandbox_id = _validate_safe_id("sandbox id", sandbox_id)
        generation = _validate_nonnegative_int("sandbox generation", sandbox_generation)
        project = _validate_positive_int("project id", project_id, maximum=(2**32) - 1)
        with self._lock():
            self._require_xfs_project_quota()
            path = self._incarnation_path(sandbox_id, generation)
            removed = False
            if os.path.lexists(path):
                _check_real_directory(
                    path,
                    "sandbox quota directory",
                    require_root_ownership=self.require_root_ownership,
                )
                actual_project, _inherits = self._project_attributes(path)
                if actual_project != project:
                    raise QuotaHelperError(
                        "sandbox quota directory belongs to another XFS project"
                    )
                trash = self.config.quota_root / (
                    f".drop-{project}-{generation}-{uuid.uuid4().hex}"
                )
                os.replace(path, trash)
                _fsync_directory(self.config.quota_root)
                shutil.rmtree(trash)
                removed = True
            self.runner(
                (
                    str(self.config.xfs_quota),
                    "-x",
                    "-c",
                    f"limit -p bsoft=0 bhard=0 {project}",
                    str(self.config.mount_root),
                )
            )
            return {
                "project_id": project,
                "removed": removed,
                "sandbox_generation": generation,
                "sandbox_id": sandbox_id,
                "state": "absent",
            }

    def list_state(self) -> dict[str, Any]:
        with self._lock():
            self._require_xfs_project_quota()
            reservations: list[dict[str, Any]] = []
            for path in sorted(
                self.config.quota_root.iterdir(),
                key=lambda candidate: candidate.name,
            ):
                if path.name in self.SHARED_ROOT_LOCK_NAMES:
                    self._require_private_owned_file(path, "shared storage lock")
                    continue
                match = _INCARNATION.fullmatch(path.name)
                if match is None:
                    raise QuotaHelperError(
                        f"unexpected entry in quota root: {path.name}"
                    )
                project, _inherits = self._project_attributes(path)
                if project == 0:
                    raise QuotaHelperError(
                        "sandbox quota directory has no XFS project assignment"
                    )
                reservations.append(
                    self._inspect_path(
                        sandbox_id=match.group(1),
                        generation=int(match.group(2)),
                        path=path,
                        expected_project=project,
                        expected_quota_mb=None,
                    )
                )
            return {"reservations": reservations, "version": HELPER_VERSION}

    def _require_private_owned_file(self, path: Path, label: str) -> None:
        try:
            info = path.lstat()
        except FileNotFoundError as exc:
            raise QuotaHelperError(f"{label} disappeared") from exc
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or (self.require_root_ownership and info.st_uid != 0)
            or info.st_mode & 0o077
        ):
            raise QuotaHelperError(f"{label} must be a private owned regular file")

    def _incarnation_path(self, sandbox_id: str, generation: int) -> Path:
        path = self.config.quota_root / f"{sandbox_id}.sandbox-{generation}"
        if path.parent != self.config.quota_root:
            raise QuotaHelperError("sandbox quota path escaped its root")
        return path

    def _require_xfs_project_quota(self) -> None:
        output = self.runner(
            (
                str(self.config.findmnt),
                "-T",
                str(self.config.quota_root),
                "-n",
                "-o",
                "TARGET,FSTYPE,OPTIONS",
            )
        )
        fields = output.strip().split(maxsplit=2)
        if (
            len(fields) != 3
            or fields[0] != str(self.config.mount_root)
            or fields[1] != "xfs"
            or not {"prjquota", "pquota"}.intersection(fields[2].split(","))
        ):
            raise QuotaHelperError(
                "hibernation storage is not the configured XFS project-quota mount"
            )

    def _project_attributes(self, path: Path) -> tuple[int, bool]:
        output = self.runner((str(self.config.xfs_io), "-c", "stat -v", str(path)))
        match = _PROJECT_STAT.search(output)
        if match is None:
            raise QuotaHelperError("xfs_io did not report a project ID")
        return int(match.group(1)), _PROJECT_INHERIT.search(output) is not None

    def _hard_limits_mb(self) -> dict[int, int]:
        output = self.runner(
            (
                str(self.config.xfs_quota),
                "-x",
                "-c",
                "report -p -b -N -n",
                str(self.config.mount_root),
            )
        )
        limits: dict[int, int] = {}
        for match in _PROJECT_REPORT.finditer(output):
            project_id = int(match.group(1))
            hard_kib = int(match.group(4))
            if hard_kib % 1024:
                raise QuotaHelperError("XFS project hard limit is not MiB aligned")
            limits[project_id] = hard_kib // 1024
        return limits

    def _inspect_path(
        self,
        *,
        sandbox_id: str,
        generation: int,
        path: Path,
        expected_project: int,
        expected_quota_mb: int | None,
    ) -> dict[str, Any]:
        actual_project, inherits_project = self._project_attributes(path)
        if actual_project != expected_project:
            raise QuotaHelperError(
                "XFS project assignment did not reach the sandbox directory"
            )
        if not inherits_project:
            raise QuotaHelperError(
                "sandbox quota directory does not inherit its XFS project ID"
            )
        limits = self._hard_limits_mb()
        hard_limit_mb = limits.get(actual_project)
        if hard_limit_mb is None or hard_limit_mb <= 0:
            raise QuotaHelperError("XFS project has no positive hard limit")
        if expected_quota_mb is not None and hard_limit_mb != expected_quota_mb:
            raise QuotaHelperError(
                "XFS project hard limit does not match the requested quota"
            )
        return {
            "hard_limit_mb": hard_limit_mb,
            "path": str(path),
            "project_id": actual_project,
            "sandbox_generation": generation,
            "sandbox_id": sandbox_id,
            "state": "ready",
        }

    @contextmanager
    def _lock(self):
        _check_real_directory(
            self.config.quota_root,
            "quota root",
            require_root_ownership=self.require_root_ownership,
        )
        root_fd = os.open(
            self.config.quota_root,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        descriptor = -1
        try:
            descriptor = os.open(
                self.LOCK_NAME,
                os.O_RDWR
                | os.O_CREAT
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_fd,
            )
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or (self.require_root_ownership and info.st_uid != 0)
                or info.st_mode & 0o077
            ):
                raise QuotaHelperError(
                    "quota lock must be a private owned regular file"
                )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            if descriptor >= 0:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)
            os.close(root_fd)


def _usage() -> str:
    return (
        "usage: ucloud-sandbox-hibernation-quota "
        "prepare SANDBOX GENERATION PROJECT_ID QUOTA_MB\n"
        "       ucloud-sandbox-hibernation-quota inspect SANDBOX GENERATION\n"
        "       ucloud-sandbox-hibernation-quota "
        "drop SANDBOX GENERATION PROJECT_ID\n"
        "       ucloud-sandbox-hibernation-quota list"
    )


def run_action(
    helper: XfsHibernationQuotaHelper,
    argv: Sequence[str],
) -> dict[str, Any]:
    if not argv:
        raise QuotaHelperError(_usage())
    action, *arguments = argv
    if action == "prepare" and len(arguments) == 4:
        return helper.prepare(*arguments)
    if action == "inspect" and len(arguments) == 2:
        return helper.inspect(*arguments)
    if action == "drop" and len(arguments) == 3:
        return helper.drop(*arguments)
    if action == "list" and not arguments:
        return helper.list_state()
    raise QuotaHelperError(_usage())


def main(
    argv: Sequence[str] | None = None,
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    require_root: bool = True,
) -> int:
    try:
        if require_root and os.geteuid() != 0:
            raise QuotaHelperError("hibernation quota helper must run as root")
        config = load_config(config_path, require_root_ownership=require_root)
        helper = XfsHibernationQuotaHelper(
            config,
            require_root_ownership=require_root,
        )
        result = run_action(helper, list(sys.argv[1:] if argv is None else argv))
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except QuotaHelperError as exc:
        print(f"hibernation quota helper: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
