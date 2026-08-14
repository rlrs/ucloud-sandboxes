from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .config import DeploymentConfig
from .storage_native_s3 import Boto3S3ObjectClient, S3ObjectStat


class RegistryMigrationS3Client(Protocol):
    def stat(self, key: str) -> S3ObjectStat | None: ...

    def put_file(self, key: str, path: Path, *, sha256: str) -> None: ...


@dataclass(frozen=True)
class RegistryStorageMigrationItem:
    relative_path: str
    key: str
    size: int
    sha256: str
    action: str
    source_device: int
    source_inode: int
    source_mtime_ns: int


@dataclass(frozen=True)
class RegistryStorageMigrationPlan:
    source_root: Path
    target_prefix: str
    items: tuple[RegistryStorageMigrationItem, ...]

    @property
    def upload_items(self) -> tuple[RegistryStorageMigrationItem, ...]:
        return tuple(item for item in self.items if item.action == "upload")

    @property
    def conflict_items(self) -> tuple[RegistryStorageMigrationItem, ...]:
        return tuple(item for item in self.items if item.action == "conflict")

    def to_dict(self) -> dict[str, object]:
        uploads = self.upload_items
        conflicts = self.conflict_items
        return {
            "sourceRoot": str(self.source_root),
            "targetPrefix": self.target_prefix,
            "sourceObjects": len(self.items),
            "sourceBytes": sum(item.size for item in self.items),
            "uploadObjects": len(uploads),
            "uploadBytes": sum(item.size for item in uploads),
            "existingObjects": sum(
                1 for item in self.items if item.action == "existing"
            ),
            "conflictObjects": len(conflicts),
            "conflicts": [item.relative_path for item in conflicts[:100]],
            "conflictsTruncated": len(conflicts) > 100,
        }


def plan_filesystem_registry_to_s3(
    client: RegistryMigrationS3Client,
    *,
    source_root: Path,
    target_prefix: str,
    max_objects: int = 1_000_000,
) -> RegistryStorageMigrationPlan:
    root = source_root.resolve()
    if not root.is_dir():
        raise ValueError(f"registry source root is not a directory: {root}")
    prefix = _registry_prefix(target_prefix)
    paths: list[Path] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"registry source contains a symbolic link: {path}")
        if path.is_file():
            paths.append(path)
            if len(paths) > max_objects:
                raise ValueError(
                    f"registry migration exceeds max_objects={max_objects}"
                )
        elif not path.is_dir():
            raise ValueError(f"registry source contains a special file: {path}")
    items: list[RegistryStorageMigrationItem] = []
    for path in paths:
        relative = path.relative_to(root).as_posix()
        with _open_regular_source(root, relative) as (descriptor, before):
            digest = _sha256_descriptor(descriptor)
            after = os.fstat(descriptor)
            if _source_identity(before) != _source_identity(after):
                raise RuntimeError(f"registry source changed while hashing: {path}")
        key = f"{prefix}/{relative}"
        remote = client.stat(key)
        if remote is None:
            action = "upload"
        elif remote.size == after.st_size and remote.sha256 == digest:
            action = "existing"
        else:
            action = "conflict"
        items.append(
            RegistryStorageMigrationItem(
                relative_path=relative,
                key=key,
                size=after.st_size,
                sha256=digest,
                action=action,
                source_device=after.st_dev,
                source_inode=after.st_ino,
                source_mtime_ns=after.st_mtime_ns,
            )
        )
    return RegistryStorageMigrationPlan(
        source_root=root,
        target_prefix=prefix,
        items=tuple(items),
    )


def execute_filesystem_registry_to_s3(
    client: RegistryMigrationS3Client,
    plan: RegistryStorageMigrationPlan,
    *,
    allow_overwrite: bool = False,
    max_concurrency: int = 8,
) -> int:
    if max_concurrency < 1:
        raise ValueError("registry migration concurrency must be positive")
    conflicts = plan.conflict_items
    if conflicts and not allow_overwrite:
        raise RuntimeError(
            "registry S3 target contains conflicting objects; refusing to overwrite"
        )
    selected = tuple(
        item
        for item in plan.items
        if item.action == "upload" or (allow_overwrite and item.action == "conflict")
    )

    def upload(item: RegistryStorageMigrationItem) -> None:
        path = plan.source_root / item.relative_path
        expected_identity = (
            item.source_device,
            item.source_inode,
            item.size,
            item.source_mtime_ns,
        )
        with _open_regular_source(
            plan.source_root,
            item.relative_path,
        ) as (descriptor, before):
            if _source_identity(before) != expected_identity:
                raise RuntimeError(f"registry source changed before upload: {path}")
            if _sha256_descriptor(descriptor) != item.sha256:
                raise RuntimeError(f"registry source changed before upload: {path}")
            client.put_file(
                item.key,
                _descriptor_path(descriptor),
                sha256=item.sha256,
            )
            if _sha256_descriptor(descriptor) != item.sha256:
                raise RuntimeError(f"registry source changed during upload: {path}")
        remote = client.stat(item.key)
        if (
            remote is None
            or remote.size != item.size
            or remote.sha256 != item.sha256
        ):
            raise RuntimeError(
                f"registry object failed post-upload verification: {item.key}"
            )

    with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
        tuple(executor.map(upload, selected))
    return len(selected)


def _registry_prefix(value: str) -> str:
    prefix = value.strip("/")
    if not prefix or any(part in {"", ".", ".."} for part in prefix.split("/")):
        raise ValueError("registry S3 target prefix is invalid")
    return prefix


def _sha256_descriptor(descriptor: int) -> str:
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    while block := os.read(descriptor, 1024 * 1024):
        digest.update(block)
    os.lseek(descriptor, 0, os.SEEK_SET)
    return digest.hexdigest()


def _source_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


@contextmanager
def _open_regular_source(
    root: Path,
    relative_path: str,
) -> Iterator[tuple[int, os.stat_result]]:
    parts = Path(relative_path).parts
    if (
        not parts
        or Path(relative_path).is_absolute()
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise RuntimeError(f"registry source path is unsafe: {relative_path!r}")
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    descriptors: list[int] = []
    try:
        descriptors.append(os.open(root, directory_flags | nofollow))
        for part in parts[:-1]:
            descriptors.append(
                os.open(
                    part,
                    directory_flags | nofollow,
                    dir_fd=descriptors[-1],
                )
            )
        descriptor = os.open(
            parts[-1],
            os.O_RDONLY | nofollow,
            dir_fd=descriptors[-1],
        )
        descriptors.append(descriptor)
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode):
            raise RuntimeError(
                f"registry source is not a regular file: {relative_path!r}"
            )
        yield descriptor, file_stat
    except OSError as exc:
        raise RuntimeError(
            f"registry source path changed or became unsafe: {relative_path!r}"
        ) from exc
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _descriptor_path(descriptor: int) -> Path:
    for root in (Path("/proc/self/fd"), Path("/dev/fd")):
        candidate = root / str(descriptor)
        if candidate.exists():
            return candidate
    raise RuntimeError("cannot upload registry source through a stable descriptor")


def _require_registry_inactive() -> None:
    completed = subprocess.run(
        [
            "systemctl",
            "is-active",
            "--quiet",
            "ucloud-sandbox-registry.service",
        ],
        check=False,
    )
    if completed.returncode == 0:
        raise RuntimeError(
            "stop ucloud-sandbox-registry.service before executing migration"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Copy a stopped filesystem-backed OCI registry into S3"
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--max-objects", type=int, default=1_000_000)
    parser.add_argument("--max-concurrency", type=int, default=8)
    parser.add_argument("--allow-overwrite", action="store_true")
    parser.add_argument("--execute", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = DeploymentConfig.from_file(args.config.resolve())
    store = config.registry_store
    if store.kind != "s3":
        raise ValueError("target deployment registry_store.kind must be s3")
    access_key = os.environ.get(store.access_key_id_env, "").strip()
    secret_key = os.environ.get(store.secret_access_key_env, "").strip()
    if not access_key or not secret_key:
        raise ValueError(
            "S3 registry credentials are missing from "
            f"{store.access_key_id_env} and {store.secret_access_key_env}"
        )
    client = Boto3S3ObjectClient(
        endpoint=store.endpoint,
        bucket=store.bucket,
        region=store.region,
        credentials={
            "access_key_id": access_key,
            "secret_access_key": secret_key,
        },
    )
    plan = plan_filesystem_registry_to_s3(
        client,
        source_root=args.source_root,
        target_prefix=store.prefix,
        max_objects=args.max_objects,
    )
    result = plan.to_dict()
    result["executed"] = bool(args.execute)
    result["uploadedObjects"] = 0
    if args.execute:
        _require_registry_inactive()
        result["uploadedObjects"] = execute_filesystem_registry_to_s3(
            client,
            plan,
            allow_overwrite=args.allow_overwrite,
            max_concurrency=args.max_concurrency,
        )
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
