#!/usr/bin/env python3
"""Snapshot the control plane's durable state onto durable storage.

Two parts, each with its own timer and retention:

  gateway      PostgreSQL (pg_dump, custom format), an online copy of every
               SQLite database in the state root, the remaining state files
               (tokens, SSH and producer keys, the provider session), and
               /etc/ucloud-sandboxes. One tar.gz per snapshot.
  chunk-index  The store node's chunk index (authoritative: S3 packs without
               it are unaddressable), copied online on the store node,
               quick_check'ed there, and streamed back gzip-compressed.

Run as root on the gateway. Snapshots hold credentials: keep --dest private
(mode 0700). A pending file is published by rename only once complete, and the
two newest snapshots are always kept. On UCloud, /work/data survives the
gateway and store VMs; their local disks do not.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile

ETC = Path("/etc/ucloud-sandboxes")
CHUNK_TOKENS = Path("/var/lib/ucloud-chunk-index")
SKIP_SUFFIXES = ("-shm", "-wal", ".lock")
SKIP_DIRS = {"gateway-locks", "image-foundation-locks", "image-pool-locks", "regen-debug", "images-contexts"}


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def is_database(path: Path) -> bool:
    with path.open("rb") as stream:
        return stream.read(16).startswith(b"SQLite format 3")


def copy_state(root: Path, target: Path) -> dict[str, str]:
    """Databases through SQLite's online backup; other files as they are."""
    kinds: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if relative.parts[0] in SKIP_DIRS or not path.is_file() or path.name.endswith(SKIP_SUFFIXES):
            continue
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix in (".sqlite", ".sqlite3") and is_database(path):
            source = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=60)
            copy = sqlite3.connect(destination)
            try:
                source.backup(copy)
            finally:
                copy.close()
                source.close()
            kinds[str(relative)] = "sqlite-online"
        else:
            shutil.copy2(path, destination)
            kinds[str(relative)] = "file"
    return kinds


def postgres_dump(dsn_file: Path, user: str, target: Path) -> None:
    # Peer authentication: pg_dump runs as the services' user; root writes the file.
    dsn = dsn_file.read_text(encoding="utf-8").strip()
    with target.open("wb") as output:
        subprocess.run(["runuser", "-u", user, "--", "pg_dump", "--format=custom", "--dbname", dsn],
                       stdout=output, check=True, timeout=1800, cwd="/")


def publish(pending: Path, target: Path, root: Path, pattern: str, keep: int) -> None:
    pending.replace(target)
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    snapshots = sorted(root.glob(pattern), reverse=True)
    for old in snapshots[max(2, keep):]:
        old.unlink()


def backup_gateway(args, root: Path, stamp: str) -> dict:
    from ucloud_sandboxes.config import DeploymentConfig
    config = DeploymentConfig.from_file(args.config)
    with tempfile.TemporaryDirectory(dir=root, prefix=".gateway-") as temporary:
        work = Path(temporary) / f"gateway-{stamp}"
        work.mkdir()
        manifest: dict = {"created_at": stamp, "deployment_id": config.deployment_id, "files": {}}
        if config.relay_postgres is not None:
            postgres_dump(Path(config.relay_postgres.dsn_file), args.service_user, work / "postgres.dump")
            manifest["files"]["postgres.dump"] = "pg_dump-custom"
        manifest["files"].update({f"state/{k}": v for k, v in copy_state(Path(config.data_root), work / "state").items()})
        for source, name in ((ETC, "etc"), (CHUNK_TOKENS, "chunk-index-tokens")):
            for path in sorted(source.glob("*")):
                if path.is_file() and not (name == "chunk-index-tokens" and not path.name.endswith(".token")):
                    (work / name).mkdir(exist_ok=True)
                    shutil.copy2(path, work / name / path.name)
                    manifest["files"][f"{name}/{path.name}"] = "file"
        manifest["sha256"] = {str(p.relative_to(work)): sha256(p) for p in sorted(work.rglob("*")) if p.is_file()}
        (work / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        pending = root / f".gateway-{stamp}.tar.gz.pending"
        with tarfile.open(pending, "w:gz", compresslevel=6) as archive:
            archive.add(work, arcname=work.name)
        os.chmod(pending, 0o600)
        with pending.open("rb") as stream:
            os.fsync(stream.fileno())
        target = root / f"gateway-{stamp}.tar.gz"
        publish(pending, target, root, "gateway-*.tar.gz", args.keep)
    return {"backup": str(target), "bytes": target.stat().st_size, "files": len(manifest["sha256"])}


REMOTE_INDEX_COPY = r"""
set -eu
umask 077
copy=$(mktemp /var/tmp/chunk-index-backup.XXXXXX)
trap 'rm -f "$copy"' EXIT
python3 - "$copy" >&2 <<'PY'
import sqlite3, sys
source = sqlite3.connect("file:%s?mode=ro" % INDEX, uri=True, timeout=60)
copy = sqlite3.connect(sys.argv[1])
source.backup(copy)
source.close()
result = copy.execute("PRAGMA quick_check").fetchone()[0]
copy.close()
if result != "ok":
    raise SystemExit("chunk index copy failed quick_check: " + result)
PY
gzip -1 -c "$copy"
"""


def backup_chunk_index(args, root: Path, stamp: str) -> dict:
    from ucloud_sandboxes.environment_config import ChunkStoreConfig
    from ucloud_sandboxes.config import DeploymentConfig
    config = DeploymentConfig.from_file(args.config)
    store = config.immutable_environments.chunk_store if config.immutable_environments else None
    if not isinstance(store, ChunkStoreConfig) or store.store_node is None or not store.store_node.serve_index:
        raise SystemExit("this deployment's chunk index is not on a store node")
    host = store.store_node.listen.rsplit(":", 1)[0]
    script = REMOTE_INDEX_COPY.replace("INDEX", repr(store.index_database))
    pending = root / f".chunk-index-{stamp}.sqlite.gz.pending"
    with pending.open("wb") as output:
        subprocess.run(
            ["runuser", "-u", args.service_user, "--", "ssh", "-o", "BatchMode=yes",
             "-o", f"UserKnownHostsFile={args.known_hosts}", "-o", "StrictHostKeyChecking=accept-new",
             "-o", "ConnectTimeout=20", "-i", str(args.ssh_key), f"{args.ssh_user}@{host}", "sh", "-s"],
            input=script.encode(), stdout=output, check=True, timeout=3600, cwd="/")
        os.fsync(output.fileno())
    os.chmod(pending, 0o600)
    with gzip.open(pending, "rb") as stream:  # The stream is complete and decodes.
        while stream.read(16 << 20):
            pass
    target = root / f"chunk-index-{stamp}.sqlite.gz"
    publish(pending, target, root, "chunk-index-*.sqlite.gz", args.keep)
    return {"backup": str(target), "bytes": target.stat().st_size, "sha256": sha256(target)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("part", choices=("gateway", "chunk-index"))
    parser.add_argument("--config", type=Path, default=ETC / "deployment.json")
    parser.add_argument("--dest", type=Path, required=True, help="private (0700) directory on durable storage")
    parser.add_argument("--keep", type=int, default=48, help="snapshots to keep (at least 2)")
    parser.add_argument("--service-user", default="ucloud")
    parser.add_argument("--ssh-user", default="ucloud")
    parser.add_argument("--ssh-key", type=Path, default=Path("/var/lib/ucloud-sandboxes/state/ssh/gateway-init"))
    parser.add_argument("--known-hosts", type=Path,
                        default=Path("/var/lib/ucloud-sandboxes/state/ssh-known-hosts/store-node"))
    args = parser.parse_args()
    root = args.dest.resolve(strict=True)
    if not root.is_dir() or root.stat().st_mode & 0o077:
        raise SystemExit("--dest must be a private (mode 0700) directory")
    with (root / f".{args.part}.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)  # A timer and an operator may overlap.
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        result = (backup_gateway if args.part == "gateway" else backup_chunk_index)(args, root, stamp)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
