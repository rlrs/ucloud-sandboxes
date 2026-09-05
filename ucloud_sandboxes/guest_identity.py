"""Resolve image identities without invoking host NSS or image executables."""

from dataclasses import dataclass
import os
from pathlib import Path
import stat


@dataclass(frozen=True)
class GuestIdentity:
    uid: int
    gid: int
    name: str = ""
    home: str = ""
    shell: str = ""


def _account_file(rootfs: Path, name: str) -> list[list[str]]:
    descriptor = os.open(rootfs, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for component in ("etc", name):
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            if component == "etc":
                flags |= os.O_DIRECTORY
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError:
                return []
            os.close(descriptor)
            descriptor = child
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("image account database must be a regular file")
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            data = handle.read(1024 * 1024 + 1)
        if len(data) > 1024 * 1024:
            raise ValueError("image account database exceeds 1 MiB")
        return [line.split(":") for line in data.decode("utf-8").splitlines()]
    except (OSError, UnicodeError) as exc:
        raise ValueError("cannot safely read image account database") from exc
    finally:
        os.close(descriptor)


def _number(value: str) -> int:
    if (
        len(value) > 10
        or not value.isascii()
        or not value.isdecimal()
        or int(value) > 2**32 - 2
    ):
        raise ValueError("OCI uid/gid is out of range")
    return int(value)


def resolve_identity(rootfs: Path, value: str) -> GuestIdentity:
    user, separator, group = value.partition(":")
    if not user or (separator and not group):
        raise ValueError(
            "image user must be a name or numeric OCI user, optionally with group"
        )
    numeric = user.isascii() and user.isdecimal()
    numeric_uid = _number(user) if numeric else None
    accounts = _account_file(rootfs, "passwd")
    match = next(
        (
            row
            for row in accounts
            if len(row) == 7
            and (row[2] == str(numeric_uid) if numeric else row[0] == user)
        ),
        None,
    )
    if match is None:
        if not numeric:
            raise ValueError(
                f"image user {user!r} is absent from /etc/passwd; specify a numeric OCI user"
            )
        uid = _number(user)
        identity = GuestIdentity(
            uid, uid, "root" if uid == 0 else "", "/root" if uid == 0 else ""
        )
    else:
        identity = GuestIdentity(
            _number(match[2]), _number(match[3]), match[0], match[5], match[6]
        )
    if separator:
        if group.isascii() and group.isdecimal():
            gid = _number(group)
        else:
            groups = _account_file(rootfs, "group")
            entry = next(
                (row for row in groups if len(row) == 4 and row[0] == group), None
            )
            if entry is None:
                raise ValueError(f"image group {group!r} is absent from /etc/group")
            gid = _number(entry[2])
        identity = GuestIdentity(
            identity.uid, gid, identity.name, identity.home, identity.shell
        )
    return identity
