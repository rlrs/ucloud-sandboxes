"""Validate an uploaded tar on the node and re-serialize what it may write.

``PUT /v1/sandboxes/{id}/archive?path=DIR`` extracts a tar (plain or gzip)
below DIR with one helper exec instead of one per file. Only regular files and
directories are accepted, by canonical relative name. The helper receives a
fresh plain tar holding just the regular files, so it never parses the
client's bytes; empty directories travel as its arguments.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import errno
from io import BytesIO
import tarfile
from typing import BinaryIO, Iterator
import zlib

from .guest_paths import _CONTROL_CHARACTERS
from .sandbox import SandboxFileTooLargeError, SandboxStartupBusyError

MAX_ARCHIVE_MEMBERS = 10_000
_MAX_NAME_BYTES = 4096
_MAX_COMPONENT_BYTES = 255
# Bounds the helper argv that names empty directories.
_MAX_DIRECTORY_ARGUMENT_BYTES = 64 * 1024
# pax and GNU long-name records are read into memory by tarfile.
_MAX_METADATA_RECORD_BYTES = 64 * 1024
_IN_MEMORY_ARCHIVE_BYTES = 1024 * 1024
_SHELL_UNAVAILABLE_EXIT = 69


class SandboxArchiveUnsupportedError(RuntimeError):
    """This worker or sandbox cannot extract archives; upload files one by one."""


def sandbox_archive_extract_script() -> str:
    # GNU and busybox tar: -m sets mtime now, -o keeps the exec identity as
    # owner. Root restores the normalized modes exactly; others apply umask 022.
    return (
        "set -eu; dir=$1; shift; "
        "command -v tar >/dev/null 2>&1 || "
        f'{{ echo "tar is unavailable" >&2; exit {_SHELL_UNAVAILABLE_EXIT}; }}; '
        '[ -d "$dir" ] || mkdir -p -- "$dir"; cd "$dir"; umask 022; '
        '[ "$#" -eq 0 ] || mkdir -p -- "$@"; '
        "exec tar -x -m -o -f -"
    )


def archive_helper_unsupported(exit_code: int, stderr: bytes, *, static: bool) -> bool:
    """Whether a failed extraction means "cannot extract", not a failed write."""
    if static:
        # A static helper older than `files extract` (installed in the rootfs
        # when the sandbox was created) rejects the operation this way.
        return exit_code == 3 and (
            b"unsupported file operation" in stderr
            or b"usage: files read|write absolute-path max-bytes | stat" in stderr
        )
    return exit_code == _SHELL_UNAVAILABLE_EXIT and b"tar is unavailable" in stderr


@dataclass(frozen=True)
class ArchivePlan:
    files: int
    directory_members: int
    total_bytes: int
    empty_directories: tuple[str, ...]


class _BoundedTarInfo(tarfile.TarInfo):
    # tarfile's (private, long-stable) per-header hook: refuse a long name or
    # pax record before tarfile reads it into memory.
    def _proc_member(self, tarfile_):
        if self.type in {tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.SOLARIS_XHDTYPE,
                         tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK} \
                and self.size > _MAX_METADATA_RECORD_BYTES:
            raise tarfile.HeaderError("archive metadata record is too large")
        return super()._proc_member(tarfile_)


def _open(source: BinaryIO) -> tarfile.TarFile:
    source.seek(0)
    gzip = source.read(2) == b"\x1f\x8b"
    source.seek(0)
    return tarfile.open(fileobj=source, mode="r:gz" if gzip else "r:", tarinfo=_BoundedTarInfo)


def _member_name(raw: str) -> str:
    if raw.startswith("/"):
        raise ValueError(f"archive member {raw!r} must be a relative path")
    if _CONTROL_CHARACTERS.search(raw):
        raise ValueError("archive member names cannot contain control characters")
    parts = [part for part in raw.split("/") if part not in {"", "."}]
    if ".." in parts:
        raise ValueError(f"archive member {raw!r} cannot contain '..'")
    name = "/".join(parts)
    if len(name.encode()) > _MAX_NAME_BYTES or any(
        len(part.encode()) > _MAX_COMPONENT_BYTES for part in parts
    ):
        raise ValueError(f"archive member {raw[:64]!r} has too long a name")
    return name


def _members(source: BinaryIO) -> Iterator[tuple[tarfile.TarFile, tarfile.TarInfo, str]]:
    try:
        with _open(source) as archive:
            for count, member in enumerate(archive, 1):
                if count > MAX_ARCHIVE_MEMBERS:
                    raise ValueError(f"archive has more than {MAX_ARCHIVE_MEMBERS} members")
                yield archive, member, _member_name(member.name)
    except (tarfile.TarError, EOFError, OSError, zlib.error) as exc:
        raise ValueError(f"invalid archive: {exc}") from exc


def plan_archive(source: BinaryIO, *, max_bytes: int) -> ArchivePlan:
    """Reject the whole archive before anything is written."""
    files: set[str] = set()
    directories: set[str] = set()
    total = 0
    for _archive, member, name in _members(source):
        if member.isdir():
            if name:
                directories.add(name)
            continue
        if not member.isreg() or member.issparse():
            kind = "link" if member.issym() or member.islnk() else "special file"
            raise ValueError(
                f"archive member {member.name!r} is a {kind}; only regular files and directories are supported"
            )
        if not name or name in files:
            raise ValueError(f"archive member {member.name!r} is not a distinct file path")
        files.add(name)
        total += member.size
        if total > max_bytes:
            raise SandboxFileTooLargeError(f"archive files exceed {max_bytes} bytes")
    parents = {name.rsplit("/", index)[0] for name in files | directories
               for index in range(1, name.count("/") + 1)}
    if conflict := sorted((files & parents) | (files & directories)):
        raise ValueError(f"archive member {conflict[0]!r} is both a file and a directory")
    empty = tuple(sorted(directories - parents))
    if sum(len(name.encode()) + 1 for name in empty) > _MAX_DIRECTORY_ARGUMENT_BYTES:
        raise ValueError("archive has too many empty directories")
    return ArchivePlan(len(files), len(directories), total, empty)


@contextmanager
def normalized_archive(source: BinaryIO, plan: ArchivePlan, spool) -> Iterator[dict]:
    """The helper's stdin: a plain tar of the planned regular files only."""
    bound = plan.total_bytes + plan.files * 2048 + 2 * tarfile.RECORDSIZE
    with (spool.staging(bound) if bound > _IN_MEMORY_ARCHIVE_BYTES else _memory()) as output:
        try:
            with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as normalized:
                for archive, member, name in _members(source):
                    if member.isreg():
                        info = tarfile.TarInfo(name)
                        info.size, info.mode = member.size, member.mode & 0o777
                        normalized.addfile(info, archive.extractfile(member))
        except OSError as exc:
            # Nothing is dispatched yet: a full staging disk is a safe retry.
            if exc.errno in {errno.ENOSPC, errno.EDQUOT}:
                raise SandboxStartupBusyError("node upload staging is waiting for disk space") from exc
            raise
        if isinstance(output, BytesIO):
            yield {"input_bytes": output.getvalue()}
        else:
            output.seek(0)
            yield {"input_file": output}


@contextmanager
def _memory() -> Iterator[BytesIO]:
    yield BytesIO()
