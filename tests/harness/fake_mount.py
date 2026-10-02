"""Copy-based stand-in for overlayfs ``mount``/``umount``/``mountpoint``.

Stdlib only; each node's wrappers run it under ``python -S``. It accepts only
the invocations ``OverlayRootfsManager`` issues.

``mount -t overlay`` materializes lower + upper into the merged directory.
``umount`` writes the merged tree's difference from lower back into a fresh
upper (deletions as ``.wh.<name>`` whiteouts) and empties the merged
directory. Between the two, upper is stale: writes reach it only on unmount,
which is when the real stack seals it. The mount table is one JSON file per
merged path in the node's table directory, so it survives node-agent
restarts like a kernel mount would.
"""

from __future__ import annotations

import filecmp
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import tempfile
import uuid

WHITEOUT = ".wh."


class MountError(Exception):
    def __init__(self, message: str, code: int = 32) -> None:
        super().__init__(message)
        self.code = code


def main(kind: str, argv: list[str], config_path: str) -> int:
    try:
        config = json.loads(Path(config_path).read_text(encoding="utf-8"))
        table = Path(config["table"])
        import faults  # Beside this file on the wrapper's path.

        action = faults.fire(config.get("faults", ""), kind, argv[1:])
        if action == "fail":
            raise MountError(f"injected {kind} failure")
        if action == "hang":
            faults.block(config["faults"], kind)
        code = _dispatch(kind, table, argv)
        if action == "hang-after":
            faults.block(config["faults"], kind)
        return code
    except MountError as exc:
        print(f"fake-{kind}: {exc}", file=sys.stderr)
        return exc.code


def _dispatch(kind: str, table: Path, argv: list[str]) -> int:
    if kind == "mount":
        return _mount(table, argv[1:])
    if kind == "umount":
        return _umount(table, argv[1:])
    if kind == "mountpoint":
        return _mountpoint(table, argv[1:])
    raise MountError(f"unsupported fake mount command: {kind}", 2)


def _entry(table: Path, merged: str) -> Path:
    key = hashlib.sha256(os.path.abspath(merged).encode("utf-8")).hexdigest()
    return table / f"{key}.json"


def _mount(table: Path, args: list[str]) -> int:
    if len(args) != 6 or args[:3] != ["-t", "overlay", "overlay"] or args[3] != "-o":
        raise MountError(f"unsupported mount invocation: {args}", 2)
    options = dict(item.partition("=")[::2] for item in args[4].split(","))
    if set(options) != {"lowerdir", "upperdir", "workdir"} or ":" in options["lowerdir"]:
        raise MountError(f"unsupported overlay options: {args[4]}", 2)
    lower, upper, work = (Path(options[name]) for name in ("lowerdir", "upperdir", "workdir"))
    merged = Path(args[5])
    entry = _entry(table, args[5])
    if entry.exists():
        raise MountError(f"{merged} is already mounted")
    for path in (lower, upper, work, merged):
        if path.is_symlink() or not path.is_dir():
            raise MountError(f"overlay path is not a directory: {path}")
    if any(merged.iterdir()):
        raise MountError(f"mountpoint is not empty: {merged}")
    original_mode = stat.S_IMODE(merged.lstat().st_mode)
    _copy_children(lower, merged)
    _apply_upper(upper, merged)
    # Overlayfs exposes the upper directory inode as the mounted root.
    os.chmod(merged, stat.S_IMODE(upper.lstat().st_mode))
    _write_json(entry, {
        "lower": str(lower), "upper": str(upper), "work": str(work),
        "merged": str(merged), "merged_mode": original_mode,
    })
    return 0


def _umount(table: Path, args: list[str]) -> int:
    if len(args) != 1:
        raise MountError(f"unsupported umount invocation: {args}", 2)
    entry = _entry(table, args[0])
    try:
        record = json.loads(entry.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise MountError(f"{args[0]}: not mounted") from None
    lower, upper, merged = (Path(record[name]) for name in ("lower", "upper", "merged"))
    staging = upper.parent / f".fake-upper-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    _diff(lower, merged, staging)
    os.chmod(staging, stat.S_IMODE(upper.lstat().st_mode))
    retired = upper.parent / f".fake-upper-retired-{uuid.uuid4().hex}"
    os.rename(upper, retired)
    os.rename(staging, upper)
    shutil.rmtree(retired)
    for child in list(merged.iterdir()):
        _remove(child)
    os.chmod(merged, record["merged_mode"])
    entry.unlink()
    return 0


def _mountpoint(table: Path, args: list[str]) -> int:
    if len(args) != 2 or args[0] != "--quiet":
        raise MountError(f"unsupported mountpoint invocation: {args}", 2)
    return 0 if _entry(table, args[1]).exists() else 32


def _copy_children(source: Path, target: Path) -> None:
    for child in source.iterdir():
        _copy(child, target / child.name)


def _copy(source: Path, target: Path) -> None:
    info = source.lstat()
    if stat.S_ISDIR(info.st_mode):
        target.mkdir()
        _copy_children(source, target)
        shutil.copystat(source, target, follow_symlinks=False)
    elif stat.S_ISLNK(info.st_mode):
        os.symlink(os.readlink(source), target)
    elif stat.S_ISREG(info.st_mode):
        shutil.copy2(source, target, follow_symlinks=False)
    # Devices, FIFOs and sockets are not modeled.


def _remove(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()


def _apply_upper(upper: Path, merged: Path) -> None:
    for child in upper.iterdir():
        target = merged / child.name
        if child.name.startswith(WHITEOUT):
            hidden = merged / child.name[len(WHITEOUT):]
            if os.path.lexists(hidden):
                _remove(hidden)
            continue
        if child.is_dir() and not child.is_symlink():
            if os.path.lexists(target) and not (target.is_dir() and not target.is_symlink()):
                _remove(target)
            if not os.path.lexists(target):
                target.mkdir()
            _apply_upper(child, target)
            shutil.copystat(child, target, follow_symlinks=False)
            continue
        if os.path.lexists(target):
            _remove(target)
        _copy(child, target)


def _same(lower: Path, merged: Path) -> bool:
    left, right = lower.lstat(), merged.lstat()
    if stat.S_IFMT(left.st_mode) != stat.S_IFMT(right.st_mode):
        return False
    if stat.S_IMODE(left.st_mode) != stat.S_IMODE(right.st_mode):
        return False
    if stat.S_ISLNK(left.st_mode):
        return os.readlink(lower) == os.readlink(merged)
    if stat.S_ISREG(left.st_mode):
        return left.st_size == right.st_size and filecmp.cmp(lower, merged, shallow=False)
    return True


def _diff(lower: Path, merged: Path, out: Path) -> bool:
    """Write merged's difference from lower into ``out``; True if any."""
    changed = False
    for child in merged.iterdir():
        base = lower / child.name
        if not os.path.lexists(base):
            _copy(child, out / child.name)
            changed = True
        elif child.is_dir() and not child.is_symlink() and base.is_dir() and not base.is_symlink():
            nested = out / child.name
            nested.mkdir()
            if _diff(base, child, nested) or not _same(base, child):
                shutil.copystat(child, nested, follow_symlinks=False)
                changed = True
            else:
                nested.rmdir()
        elif not _same(base, child):
            _copy(child, out / child.name)
            changed = True
    for child in lower.iterdir():
        if not os.path.lexists(merged / child.name):
            (out / (WHITEOUT + child.name)).touch(mode=0o600)
            changed = True
    return changed


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".fake-mount-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
