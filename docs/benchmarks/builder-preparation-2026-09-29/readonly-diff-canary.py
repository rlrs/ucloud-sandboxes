#!/usr/bin/env python3
"""Root-only, temporary-file canary; select the candidate with PYTHONPATH.

Run: PYTHONPATH=/path/to/selected/runtime python3 readonly-diff-canary.py
Only the sanitized result and output fingerprint are printed. No network,
registry, Docker, or persistent runtime configuration is accessed.
"""

import hashlib
import json
import os
from pathlib import Path
import stat
import sys
from tempfile import TemporaryDirectory
from unittest.mock import patch


def fixture(root):
    layers = root / "first", root / "second"
    for layer in layers:
        (layer / "app/readonly/nested").mkdir(parents=True)
    first, second = layers
    (first / "app/original").write_bytes(b"original hardlinked inode\n")
    os.link(first / "app/original", first / "app/retained-link")
    (first / "app/readonly/nested/lower").write_bytes(b"retained lower file\n")
    (second / "app/original").write_bytes(b"upper replacement\n")
    executable = second / "app/readonly/nested/executable"
    executable.write_bytes(b"#!/bin/sh\nexit 0\n")
    os.link(executable, second / "app/readonly/nested/hardlink")
    (second / "app/readonly/relative").symlink_to("nested/executable")
    (second / "app/readonly/dangling").symlink_to("missing")
    (second / "app/readonly/absolute").symlink_to("/missing/readonly-diff-canary")
    os.setxattr(first / "app/readonly", "user.removed-in-upper", b"old")
    os.setxattr(second / "app/readonly", "user.directory", b"preserved")
    os.setxattr(executable, "user.payload", b"preserved")
    for index, layer in enumerate(layers):
        paths = [layer, *sorted(layer.rglob("*"))]
        # Establish all metadata after writing/linking; directory permissions
        # deliberately forbid mutation by their non-root owner as well.
        for path in paths:
            info = path.lstat()
            os.chown(path, 12345 + index, 23456 + index, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                path.chmod(0o500 if path.name == "nested" else 0o555)
            elif stat.S_ISREG(info.st_mode):
                path.chmod(0o4550 if path.name in {"executable", "hardlink"} else 0o440)
            os.utime(path, ns=(1_700_000_000_000_000_000, 1_600_000_000_000_000_000 + index),
                     follow_symlinks=False)
    return layers


def snapshot(root):
    entries, hardlinks = {}, {}
    for path in [root, *sorted(root.rglob("*"))]:
        name, info = str(path.relative_to(root)), path.lstat()
        entry = {"mode": info.st_mode, "uid": info.st_uid, "gid": info.st_gid,
                 "mtime_ns": info.st_mtime_ns,
                 "xattrs": {key: os.getxattr(path, key, follow_symlinks=False).hex()
                            for key in sorted(os.listxattr(path, follow_symlinks=False))}}
        if stat.S_ISREG(info.st_mode):
            entry["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
            entry["size"] = info.st_size
            hardlinks.setdefault((info.st_dev, info.st_ino), []).append(name)
        elif stat.S_ISLNK(info.st_mode):
            entry["target"] = os.readlink(path)
        elif not stat.S_ISDIR(info.st_mode):
            raise AssertionError("unexpected fixture entry type")
        entries[name] = entry
    return {"entries": entries,
            "hardlinks": sorted(sorted(names) for names in hardlinks.values() if len(names) > 1)}


def require(condition):
    if not condition:
        raise AssertionError("canary invariant failed")


def run():
    from ucloud_sandboxes import environment_builder

    with TemporaryDirectory(prefix="ucloud-readonly-diff-canary-") as raw:
        root = Path(raw)
        copy_layers = fixture(root / "copy-source")
        move_layers = fixture(root / "move-source")
        before = [snapshot(layer) for layer in copy_layers]
        require(before == [snapshot(layer) for layer in move_layers])
        executable = move_layers[1] / "app/readonly/nested/executable"
        moved_identity = executable.stat().st_dev, executable.stat().st_ino
        environment_builder.squash_layer_diffs(copy_layers, root / "copied")
        require(before == [snapshot(layer) for layer in copy_layers])
        with patch.object(environment_builder, "_copy_entry", side_effect=AssertionError("unexpected payload copy")):
            environment_builder.squash_layer_diffs(move_layers, root / "moved", consume_private_diffs=True)
        copied, moved = snapshot(root / "copied"), snapshot(root / "moved")
        require(copied == moved)
        output = root / "moved"
        executable = output / "app/readonly/nested/executable"
        info = executable.stat()
        require((info.st_dev, info.st_ino) == moved_identity)
        require((info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) == (12346, 23457, 0o4550))
        require(info.st_ino == (output / "app/readonly/nested/hardlink").stat().st_ino)
        require(stat.S_IMODE((output / "app/readonly").stat().st_mode) == 0o555)
        require(stat.S_IMODE((output / "app/readonly/nested").stat().st_mode) == 0o500)
        require((output / "app/original").read_bytes() == b"upper replacement\n")
        require((output / "app/retained-link").read_bytes() == b"original hardlinked inode\n")
        require((output / "app/original").stat().st_ino != (output / "app/retained-link").stat().st_ino)
        require(not any(path.is_file() or path.is_symlink()
                        for layer in move_layers for path in layer.rglob("*")))
        fingerprint = hashlib.sha256(json.dumps(moved, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    # TemporaryDirectory has completed cleanup before success is reported.
    return {"ok": True, "sha256": fingerprint}


def main():
    if not sys.platform.startswith("linux") or os.geteuid() != 0:
        print(json.dumps({"ok": False, "error": "linux_root_required"}, sort_keys=True))
        return 2
    try:
        result = run()
    except Exception as exc:
        # Never print paths, file payloads, environment values or tracebacks.
        print(json.dumps({"ok": False, "error": type(exc).__name__}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
