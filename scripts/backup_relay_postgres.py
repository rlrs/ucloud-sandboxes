#!/usr/bin/env python3
"""Back up the local relay PostgreSQL container onto its /backups bind mount.

Run as the host administrator. The directory must be the host source of the
container's /backups mount. Dumps contain registration tokens and model results;
keep the directory private. This is a snapshot backup, not streaming replication.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import os
from pathlib import Path
import subprocess
import uuid


def backup(args) -> Path:
    root = args.backup_dir.resolve(strict=True)
    if not root.is_dir() or root.stat().st_mode & 0o077:
        raise ValueError("backup directory must be private (mode 0700)")
    # The unit and an operator may request a snapshot simultaneously.
    with (root / ".backup.lock").open("a") as lock:
        os.chmod(lock.name, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        name = f"relay-{stamp}-{uuid.uuid4().hex[:8]}.dump"
        pending = root / ("." + name + ".pending")
        target = root / name
        try:
            subprocess.run([
                "docker", "exec", "--user", "postgres", args.container,
                "pg_dump", "--username", args.user, "--dbname", args.database,
                "--format=custom", "--file", "/backups/" + pending.name,
            ], check=True, timeout=1800)
            pending.chmod(0o600)
            with pending.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
                os.fsync(stream.fileno())
            pending.replace(target)
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            # Never prune until the new dump has been durably published. Always
            # preserve two complete snapshots, even if they exceed the budget.
            snapshots = sorted(root.glob("relay-*.dump"), reverse=True)
            remaining_bytes = sum(p.stat().st_size for p in snapshots)
            while len(snapshots) > 2 and (
                len(snapshots) > args.keep or remaining_bytes > args.max_bytes
            ):
                old = snapshots.pop()
                remaining_bytes -= old.stat().st_size
                old.unlink()
            print(f"backup={target} bytes={target.stat().st_size} sha256={digest}")
            return target
        finally:
            pending.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup-dir", type=Path, required=True)
    parser.add_argument("--container", default="ucloud-relay-postgres")
    parser.add_argument("--user", default="ucloud_relay")
    parser.add_argument("--database", default="ucloud_relay")
    parser.add_argument("--keep", type=int, default=48)
    parser.add_argument("--max-bytes", type=int, default=10 * 1024**3)
    args = parser.parse_args()
    if args.keep < 2 or args.max_bytes < 1:
        parser.error("keep must be at least two and max-bytes must be positive")
    backup(args)


if __name__ == "__main__":
    main()
