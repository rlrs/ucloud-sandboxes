from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import tarfile
import tempfile
from threading import Lock, Semaphore
import time
from typing import Any, Iterator, Protocol
from uuid import uuid4

from .direct_warden import (
    CommandRunner,
    DirectSandbox,
    DirectWardenError,
    SubprocessCommandRunner,
)


_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_ROOTFS_SCHEMA = 2
_OVERLAY2_ROOTFS_SCHEMA = 1
_OVERLAY_METADATA = ".ucloud-overlay.json"
_OVERLAY_METADATA_SCHEMA = 1


def _canonical_json(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class DockerImageConfig:
    entrypoint: tuple[str, ...] = ()
    command: tuple[str, ...] = ()
    env: tuple[str, ...] = ()
    working_dir: str = ""
    user: str = ""

    def __post_init__(self) -> None:
        for label, values in (
            ("entrypoint", self.entrypoint),
            ("command", self.command),
            ("env", self.env),
        ):
            if any(not isinstance(value, str) or "\0" in value for value in values):
                raise ValueError(f"Docker image {label} is invalid")
        if "\0" in self.working_dir or "\0" in self.user:
            raise ValueError("Docker image process configuration is invalid")

    @classmethod
    def from_inspection(cls, raw: object) -> DockerImageConfig:
        if not isinstance(raw, dict):
            raise ValueError("Docker image Config is invalid")

        def string_tuple(name: str) -> tuple[str, ...]:
            value = raw.get(name)
            if value is None:
                return ()
            if not isinstance(value, list) or not all(
                isinstance(item, str) for item in value
            ):
                raise ValueError(f"Docker image Config.{name} is invalid")
            return tuple(value)

        return cls(
            entrypoint=string_tuple("Entrypoint"),
            command=string_tuple("Cmd"),
            env=string_tuple("Env"),
            working_dir=str(raw.get("WorkingDir") or ""),
            user=str(raw.get("User") or ""),
        )

    @classmethod
    def from_dict(cls, raw: object) -> DockerImageConfig:
        if not isinstance(raw, dict) or set(raw) != {
            "command",
            "entrypoint",
            "env",
            "user",
            "working_dir",
        }:
            raise ValueError("materialized Docker image config is invalid")
        for name in ("command", "entrypoint", "env"):
            if not isinstance(raw[name], list):
                raise ValueError("materialized Docker image config is invalid")
        return cls(
            command=tuple(raw["command"]),
            entrypoint=tuple(raw["entrypoint"]),
            env=tuple(raw["env"]),
            user=str(raw["user"]),
            working_dir=str(raw["working_dir"]),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "command": list(self.command),
            "entrypoint": list(self.entrypoint),
            "env": list(self.env),
            "user": self.user,
            "working_dir": self.working_dir,
        }


@dataclass(frozen=True)
class MaterializedRootfs:
    image_ref: str
    image_id: str
    rootfs_identity_sha256: str
    rootfs: Path
    image_config: DockerImageConfig


class RootfsExtractor(Protocol):
    def extract(self, archive: Path, destination: Path) -> None: ...


class GnuTarRootfsExtractor:
    """Validate Docker's export stream before privileged GNU tar extraction."""

    def __init__(
        self,
        *,
        runner: CommandRunner | None = None,
        tar_binary: str = "tar",
        sync_binary: str = "sync",
    ) -> None:
        self.runner = runner or SubprocessCommandRunner()
        self.tar_binary = tar_binary
        self.sync_binary = sync_binary

    def extract(self, archive: Path, destination: Path) -> None:
        self._validate_archive(archive)
        self._checked(
            self.tar_binary,
            "--extract",
            f"--file={archive}",
            f"--directory={destination}",
            "--numeric-owner",
            "--same-owner",
            "--same-permissions",
            "--xattrs",
            "--delay-directory-restore",
        )
        self._checked(self.sync_binary, "-f", str(destination))

    @staticmethod
    def _validate_archive(archive: Path) -> None:
        symlinks: set[PurePosixPath] = set()
        with tarfile.open(archive, mode="r:*") as source:
            for member in source:
                path = PurePosixPath(member.name)
                if (
                    not member.name
                    or path.is_absolute()
                    or ".." in path.parts
                    or "\\" in member.name
                ):
                    raise DirectWardenError(
                        f"image export contains an unsafe path: {member.name!r}"
                    )
                normalized = PurePosixPath(
                    *(part for part in path.parts if part not in {"", "."})
                )
                if not normalized.parts:
                    continue
                for parent in normalized.parents:
                    if parent in symlinks:
                        raise DirectWardenError(
                            "image export writes through an archived symlink"
                        )
                if member.issym():
                    symlinks.add(normalized)
                if member.islnk():
                    target = PurePosixPath(member.linkname)
                    if target.is_absolute() or ".." in target.parts:
                        raise DirectWardenError(
                            "image export contains an unsafe hard link"
                        )

    def _checked(self, *argv: str) -> None:
        result = self.runner.run(argv, timeout=600)
        if result.returncode != 0:
            raise DirectWardenError(
                f"rootfs extraction command failed: {result.argv!r}; "
                f"stdout={result.stdout!r}; stderr={result.stderr!r}"
            )


class DockerRootfsStore:
    """Use Docker for image resolution/export, never for sandbox tasks."""

    MANIFEST = "manifest.json"
    COMPLETE = "COMPLETE"
    EXPORT_LABEL = "dev.ucloud-sandboxes.image-export=true"

    def __init__(
        self,
        root: Path,
        *,
        runner: CommandRunner | None = None,
        extractor: RootfsExtractor | None = None,
        docker_binary: str = "docker",
        max_concurrent_exports: int = 4,
    ) -> None:
        if not root.is_absolute():
            raise ValueError("rootfs store root must be absolute")
        self.root = root
        self.runner = runner or SubprocessCommandRunner()
        self.extractor = extractor or GnuTarRootfsExtractor()
        self.docker_binary = docker_binary
        if max_concurrent_exports < 1:
            raise ValueError("max_concurrent_exports must be positive")
        self.max_concurrent_exports = int(max_concurrent_exports)
        self._export_slots = Semaphore(self.max_concurrent_exports)
        self._operation_guard = Lock()
        self._active_exports = 0
        self._waiting_exports = 0
        self._exports_reconcile_guard = Lock()
        self._exports_reconciled = False
        self.images = root / "images"
        self.locks = root / "locks"
        for path in (root, self.images, self.locks):
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            self._require_private_directory(path)

    def materialize(self, image_ref: str) -> MaterializedRootfs:
        image_ref = str(image_ref).strip()
        if not image_ref or "\0" in image_ref:
            raise ValueError("image_ref is invalid")
        inspection = self._checked(
            self.docker_binary,
            "image",
            "inspect",
            image_ref,
        )
        try:
            raw = json.loads(inspection)
            image_id = str(raw[0]["Id"])
            image_config = DockerImageConfig.from_inspection(raw[0].get("Config"))
        except (
            IndexError,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            raise DirectWardenError(
                "docker image inspect returned invalid JSON"
            ) from exc
        if not image_id.startswith("sha256:") or not _DIGEST.fullmatch(image_id[7:]):
            raise DirectWardenError("docker image inspect returned an invalid image ID")
        digest = image_id[7:]
        # The digest lock deduplicates identical images. Distinct cold images
        # may export concurrently; a shared process lease prevents a restarted
        # Warden's orphan reconciliation from deleting their exporter
        # containers. The old global exclusive lock serialized every distinct
        # image in a burst and produced multi-minute queues.
        with self._locked(digest):
            existing = self._load_complete(digest, image_ref=image_ref)
            if existing is not None:
                return existing
            self._ensure_export_reconciliation()
            self._discard_pending_exports(digest)
            with self._operation_guard:
                self._waiting_exports += 1
            self._export_slots.acquire()
            with self._operation_guard:
                self._waiting_exports -= 1
                self._active_exports += 1
            try:
                with self._locked("exports", shared=True):
                    return self._export(
                        digest,
                        image_id=image_id,
                        image_ref=image_ref,
                        image_config=image_config,
                    )
            except Exception:
                self._discard_pending_exports(digest)
                raise
            finally:
                with self._operation_guard:
                    self._active_exports -= 1
                self._export_slots.release()

    def operation_snapshot(self) -> dict[str, int]:
        with self._operation_guard:
            return {
                "active_operations": self._active_exports,
                "waiting_operations": self._waiting_exports,
                "max_concurrent_operations": self.max_concurrent_exports,
            }

    def _ensure_export_reconciliation(self) -> None:
        with self._exports_reconcile_guard:
            if self._exports_reconciled:
                return
            with self._locked("exports"):
                self._reconcile_export_containers_locked()
            self._exports_reconciled = True

    def reconcile_export_containers(self) -> tuple[str, ...]:
        """Remove exporter containers orphaned by an earlier Warden process."""
        with self._locked("exports"):
            removed = self._reconcile_export_containers_locked()
        with self._exports_reconcile_guard:
            self._exports_reconciled = True
        return removed

    def _reconcile_export_containers_locked(self) -> tuple[str, ...]:
        listing = self._checked(
            self.docker_binary,
            "ps",
            "--all",
            f"--filter=label={self.EXPORT_LABEL}",
            "--format={{.ID}} {{.State}}",
        )
        removed: list[str] = []
        for line in listing.splitlines():
            fields = line.split()
            if len(fields) != 2 or not re.fullmatch(r"[0-9a-f]{12,64}", fields[0]):
                raise DirectWardenError("docker returned invalid exporter inventory")
            container_id, state = fields
            if state not in {"created", "exited", "dead"}:
                raise DirectWardenError(
                    "refusing to remove an exporter container in state "
                    f"{state!r}: {container_id}"
                )
            self._checked(
                self.docker_binary,
                "rm",
                "--force",
                "--volumes",
                container_id,
            )
            removed.append(container_id)
        return tuple(removed)

    def _export(
        self,
        digest: str,
        *,
        image_id: str,
        image_ref: str,
        image_config: DockerImageConfig,
    ) -> MaterializedRootfs:
        pending = self.images / f".{digest}.{uuid4().hex}.pending"
        pending.mkdir(mode=0o700)
        rootfs = pending / "rootfs"
        rootfs.mkdir(mode=0o755)
        archive = pending / "rootfs.tar"
        container_id = ""
        try:
            container_id = self._checked(
                self.docker_binary,
                "create",
                "--network=none",
                "--entrypoint=/bin/true",
                f"--label={self.EXPORT_LABEL}",
                image_id,
            ).strip()
            if not re.fullmatch(r"[0-9a-f]{12,64}", container_id):
                raise DirectWardenError(
                    "docker create returned an invalid container ID"
                )
            self._checked(
                self.docker_binary,
                "export",
                f"--output={archive}",
                container_id,
                timeout=600,
            )
            self.extractor.extract(archive, rootfs)
        finally:
            if container_id:
                self.runner.run(
                    (
                        self.docker_binary,
                        "rm",
                        "--force",
                        "--volumes",
                        container_id,
                    ),
                    timeout=60,
                )
        archive.unlink()
        identity = _sha256(
            b"ucloud-docker-export-rootfs-v1\0" + image_id.encode("ascii")
        )
        manifest = {
            "created_ns": time.time_ns(),
            "image_id": image_id,
            "image_config": image_config.to_dict(),
            "image_ref": image_ref,
            "rootfs_identity_sha256": identity,
            "schema": _ROOTFS_SCHEMA,
        }
        self._atomic_write(pending / self.MANIFEST, _canonical_json(manifest) + b"\n")
        self._atomic_write(
            pending / self.COMPLETE,
            _canonical_json(
                {
                    "image_id": image_id,
                    "rootfs_identity_sha256": identity,
                    "schema": _ROOTFS_SCHEMA,
                }
            )
            + b"\n",
        )
        self._fsync_directory(pending)
        target = self.images / digest
        try:
            pending.replace(target)
        except FileExistsError:
            shutil.rmtree(pending)
            existing = self._load_complete(digest, image_ref=image_ref)
            if existing is None:
                raise DirectWardenError("concurrent rootfs publication is incomplete")
            return existing
        self._fsync_directory(self.images)
        return MaterializedRootfs(
            image_ref=image_ref,
            image_id=image_id,
            rootfs_identity_sha256=identity,
            rootfs=target / "rootfs",
            image_config=image_config,
        )

    def _load_complete(
        self,
        digest: str,
        *,
        image_ref: str,
    ) -> MaterializedRootfs | None:
        target = self.images / digest
        if not target.exists():
            return None
        self._require_private_directory(target)
        rootfs = target / "rootfs"
        self._require_real_directory(rootfs)
        try:
            manifest = json.loads((target / self.MANIFEST).read_text(encoding="ascii"))
            marker = json.loads((target / self.COMPLETE).read_text(encoding="ascii"))
            image_config = DockerImageConfig.from_dict(manifest.get("image_config"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DirectWardenError("materialized rootfs metadata is invalid") from exc
        except (TypeError, ValueError) as exc:
            raise DirectWardenError("materialized rootfs image config is invalid") from exc
        image_id = f"sha256:{digest}"
        expected_identity = _sha256(
            b"ucloud-docker-export-rootfs-v1\0" + image_id.encode("ascii")
        )
        if (
            manifest.get("schema") != _ROOTFS_SCHEMA
            or manifest.get("image_id") != image_id
            or manifest.get("rootfs_identity_sha256") != expected_identity
            or marker
            != {
                "image_id": image_id,
                "rootfs_identity_sha256": expected_identity,
                "schema": _ROOTFS_SCHEMA,
            }
        ):
            raise DirectWardenError("materialized rootfs identity is invalid")
        return MaterializedRootfs(
            image_ref=image_ref,
            image_id=image_id,
            rootfs_identity_sha256=expected_identity,
            rootfs=rootfs,
            image_config=image_config,
        )

    def _discard_pending_exports(self, digest: str) -> None:
        pattern = re.compile(rf"\.{re.escape(digest)}\.[0-9a-f]{{32}}\.pending\Z")
        for path in self.images.iterdir():
            if pattern.fullmatch(path.name):
                self._require_private_directory(path)
                shutil.rmtree(path)
        self._fsync_directory(self.images)

    def _checked(
        self,
        *argv: str,
        timeout: float = 60,
    ) -> str:
        result = self.runner.run(argv, timeout=timeout)
        if result.returncode != 0:
            raise DirectWardenError(
                f"image command failed ({result.returncode}): {result.argv!r}; "
                f"stdout={result.stdout!r}; stderr={result.stderr!r}"
            )
        return result.stdout.strip()

    @contextmanager
    def _locked(self, digest: str, *, shared: bool = False) -> Iterator[None]:
        descriptor = os.open(
            self.locks / f"{digest}.lock",
            os.O_RDWR
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            fcntl.flock(
                descriptor,
                fcntl.LOCK_SH if shared else fcntl.LOCK_EX,
            )
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        descriptor, raw = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        temporary = Path(raw)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(path)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _require_real_directory(path: Path) -> None:
        if not path.is_dir() or path.is_symlink():
            raise DirectWardenError("rootfs path must be a real directory")

    @classmethod
    def _require_private_directory(cls, path: Path) -> None:
        cls._require_real_directory(path)
        info = path.lstat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise DirectWardenError("rootfs store directory must be owned and private")


class DockerOverlay2RootfsStore(DockerRootfsStore):
    """Mount Docker's immutable overlay2 layers without flattening the image.

    Docker remains the OCI image owner, but it never owns a sandbox task. A
    digest-specific local tag leases every image used by the runtime so Docker
    pruning cannot remove layers below a live or parked sandbox.
    """

    PIN_REPOSITORY = "ucloud-sandbox-rootfs-cache"

    def __init__(
        self,
        root: Path,
        *,
        runner: CommandRunner | None = None,
        docker_binary: str = "docker",
        docker_root: Path | None = None,
        mount_binary: str = "mount",
        mountpoint_binary: str = "mountpoint",
        umount_binary: str = "umount",
    ) -> None:
        super().__init__(
            root,
            runner=runner,
            docker_binary=docker_binary,
            max_concurrent_exports=32,
        )
        if docker_root is not None and not docker_root.is_absolute():
            raise ValueError("Docker data root must be absolute")
        self._configured_docker_root = docker_root
        self._resolved_docker_root: Path | None = None
        self._docker_root_guard = Lock()
        self._driver_validated = False
        self.mount_binary = mount_binary
        self.mountpoint_binary = mountpoint_binary
        self.umount_binary = umount_binary
        # Do not collide with an exported rootfs from an earlier release. The
        # mounted image view has a different durability contract even though
        # its semantic rootfs identity deliberately remains compatible.
        self.images = root / "overlay2-images"
        self.images.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._require_private_directory(self.images)

    def materialize(self, image_ref: str) -> MaterializedRootfs:
        image_ref = str(image_ref).strip()
        if not image_ref or "\0" in image_ref:
            raise ValueError("image_ref is invalid")
        image_id, image_config, layers = self._inspect_overlay2(image_ref)
        digest = image_id[7:]
        with self._locked(digest):
            target = self.images / digest
            if target.exists():
                existing = self._load_overlay2_complete(
                    digest,
                    image_ref=image_ref,
                    image_config=image_config,
                )
                if existing is None:
                    self._discard_overlay2_target(target)
                else:
                    self._ensure_overlay2_mount(existing.rootfs, layers)
                    return existing
            self._pin_image(image_id)
            target.mkdir(mode=0o700)
            rootfs = target / "rootfs"
            rootfs.mkdir(mode=0o755)
            identity = self._rootfs_identity(image_id)
            mounted = False
            try:
                self._mount_overlay2(rootfs, layers)
                mounted = True
                manifest = {
                    "created_ns": time.time_ns(),
                    "image_id": image_id,
                    "image_config": image_config.to_dict(),
                    "image_ref": image_ref,
                    "rootfs_identity_sha256": identity,
                    "schema": _OVERLAY2_ROOTFS_SCHEMA,
                    "store": "docker-overlay2",
                }
                self._atomic_write(
                    target / self.MANIFEST,
                    _canonical_json(manifest) + b"\n",
                )
                self._atomic_write(
                    target / self.COMPLETE,
                    _canonical_json(
                        {
                            "image_id": image_id,
                            "rootfs_identity_sha256": identity,
                            "schema": _OVERLAY2_ROOTFS_SCHEMA,
                            "store": "docker-overlay2",
                        }
                    )
                    + b"\n",
                )
                self._fsync_directory(target)
                self._fsync_directory(self.images)
            except Exception as exc:
                if mounted:
                    result = self.runner.run(
                        (self.umount_binary, str(rootfs)),
                        timeout=60,
                    )
                    if result.returncode != 0:
                        raise DirectWardenError(
                            "overlay2 image publication failed and its mount "
                            f"could not be released: {result.stderr or result.stdout}"
                        ) from exc
                shutil.rmtree(target, ignore_errors=True)
                raise
            return MaterializedRootfs(
                image_ref=image_ref,
                image_id=image_id,
                rootfs_identity_sha256=identity,
                rootfs=rootfs,
                image_config=image_config,
            )

    def reconcile_export_containers(self) -> tuple[str, ...]:
        """Clean legacy exporters and restore every durable Docker image lease."""
        self._validate_docker_driver()
        self._docker_overlay2_root()
        removed = super().reconcile_export_containers()
        for target in self.images.iterdir():
            if target.name.startswith("."):
                continue
            if not _DIGEST.fullmatch(target.name):
                raise DirectWardenError("overlay2 image cache contains an invalid entry")
            self._require_private_directory(target)
            try:
                marker = json.loads(
                    (target / self.COMPLETE).read_text(encoding="ascii")
                )
            except FileNotFoundError:
                self._discard_overlay2_target(target)
                continue
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise DirectWardenError("overlay2 rootfs marker is invalid") from exc
            image_id = f"sha256:{target.name}"
            if (
                not isinstance(marker, dict)
                or marker.get("image_id") != image_id
                or marker.get("store") != "docker-overlay2"
                or marker.get("schema") != _OVERLAY2_ROOTFS_SCHEMA
            ):
                raise DirectWardenError("overlay2 rootfs marker identity is invalid")
            self._pin_image(image_id)
        return removed

    def _pin_image(self, image_id: str) -> None:
        self._checked(
            self.docker_binary,
            "image",
            "tag",
            image_id,
            f"{self.PIN_REPOSITORY}:{image_id[7:]}",
        )

    def _inspect_overlay2(
        self,
        image_ref: str,
    ) -> tuple[str, DockerImageConfig, tuple[Path, ...]]:
        inspection = self._checked(
            self.docker_binary,
            "image",
            "inspect",
            image_ref,
        )
        try:
            raw = json.loads(inspection)
            record = raw[0]
            image_id = str(record["Id"])
            image_config = DockerImageConfig.from_inspection(record.get("Config"))
            graph = record["GraphDriver"]
            graph_name = str(graph["Name"])
            graph_data = graph["Data"]
            upper = str(graph_data["UpperDir"])
            lower = str(graph_data.get("LowerDir") or "")
        except (
            IndexError,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            raise DirectWardenError(
                "docker image inspect returned invalid overlay2 metadata"
            ) from exc
        if not image_id.startswith("sha256:") or not _DIGEST.fullmatch(image_id[7:]):
            raise DirectWardenError("docker image inspect returned an invalid image ID")
        if graph_name != "overlay2":
            raise DirectWardenError(
                f"direct runtime requires Docker overlay2, got {graph_name!r}"
            )
        raw_layers = (upper, *(item for item in lower.split(":") if item))
        if not raw_layers or not raw_layers[0]:
            raise DirectWardenError("Docker overlay2 image has no immutable layers")
        layers = tuple(self._validate_overlay2_layer(item) for item in raw_layers)
        option = "ro,lowerdir=" + ":".join(str(item) for item in layers)
        try:
            mount_option_limit = os.sysconf("SC_PAGE_SIZE") - 512
        except (OSError, ValueError):
            mount_option_limit = 3584
        if len(os.fsencode(option)) > mount_option_limit:
            raise DirectWardenError(
                "Docker overlay2 layer stack exceeds the kernel mount option limit"
            )
        return image_id, image_config, layers

    def _validate_docker_driver(self) -> None:
        if self._driver_validated:
            return
        payload = self._checked(
            self.docker_binary,
            "info",
            "--format={{json .Driver}}",
        )
        try:
            driver = json.loads(payload)
        except (TypeError, json.JSONDecodeError) as exc:
            raise DirectWardenError("docker info returned an invalid driver") from exc
        if driver != "overlay2":
            raise DirectWardenError(
                f"direct runtime requires Docker overlay2, got {driver!r}"
            )
        self._driver_validated = True

    def _validate_overlay2_layer(self, raw: str) -> Path:
        if not raw or "\0" in raw or "," in raw or ":" in raw or "\\" in raw:
            raise DirectWardenError("Docker overlay2 returned an unsafe layer path")
        candidate = Path(raw)
        if not candidate.is_absolute():
            raise DirectWardenError("Docker overlay2 returned a relative layer path")
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(self._docker_overlay2_root())
        except (OSError, ValueError) as exc:
            raise DirectWardenError(
                "Docker overlay2 layer escaped the Docker data root"
            ) from exc
        self._require_real_directory(resolved)
        info = resolved.stat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise DirectWardenError(
                "Docker overlay2 layer must be root-owned and non-writable"
            )
        return resolved

    def _docker_overlay2_root(self) -> Path:
        with self._docker_root_guard:
            if self._resolved_docker_root is not None:
                return self._resolved_docker_root / "overlay2"
            configured = self._configured_docker_root
            if configured is None:
                payload = self._checked(
                    self.docker_binary,
                    "info",
                    "--format={{json .DockerRootDir}}",
                )
                try:
                    configured = Path(str(json.loads(payload)))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise DirectWardenError(
                        "docker info returned an invalid data root"
                    ) from exc
            if not configured.is_absolute():
                raise DirectWardenError("Docker data root must be absolute")
            try:
                resolved = configured.resolve(strict=True)
            except OSError as exc:
                raise DirectWardenError("Docker data root is unavailable") from exc
            self._require_real_directory(resolved)
            info = resolved.stat()
            if info.st_uid != os.geteuid() or info.st_mode & 0o022:
                raise DirectWardenError("Docker data root is not safely owned")
            self._resolved_docker_root = resolved
            overlay2 = resolved / "overlay2"
            self._require_real_directory(overlay2)
            overlay_info = overlay2.stat()
            if overlay_info.st_uid != os.geteuid() or overlay_info.st_mode & 0o022:
                raise DirectWardenError("Docker overlay2 root is not safely owned")
            return overlay2

    def _load_overlay2_complete(
        self,
        digest: str,
        *,
        image_ref: str,
        image_config: DockerImageConfig,
    ) -> MaterializedRootfs | None:
        target = self.images / digest
        if not target.exists():
            return None
        self._require_private_directory(target)
        rootfs = target / "rootfs"
        self._require_real_directory(rootfs)
        try:
            manifest = json.loads((target / self.MANIFEST).read_text(encoding="ascii"))
            marker = json.loads((target / self.COMPLETE).read_text(encoding="ascii"))
        except FileNotFoundError:
            return None
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DirectWardenError("overlay2 rootfs metadata is invalid") from exc
        image_id = f"sha256:{digest}"
        identity = self._rootfs_identity(image_id)
        if (
            not isinstance(manifest, dict)
            or manifest.get("schema") != _OVERLAY2_ROOTFS_SCHEMA
            or manifest.get("store") != "docker-overlay2"
            or manifest.get("image_id") != image_id
            or manifest.get("rootfs_identity_sha256") != identity
            or marker
            != {
                "image_id": image_id,
                "rootfs_identity_sha256": identity,
                "schema": _OVERLAY2_ROOTFS_SCHEMA,
                "store": "docker-overlay2",
            }
        ):
            raise DirectWardenError("overlay2 rootfs identity is invalid")
        return MaterializedRootfs(
            image_ref=image_ref,
            image_id=image_id,
            rootfs_identity_sha256=identity,
            rootfs=rootfs,
            image_config=image_config,
        )

    def _discard_overlay2_target(self, target: Path) -> None:
        rootfs = target / "rootfs"
        if rootfs.exists():
            mounted = self.runner.run(
                (self.mountpoint_binary, "--quiet", str(rootfs)),
                timeout=60,
            )
            if mounted.returncode == 0:
                result = self.runner.run(
                    (self.umount_binary, str(rootfs)),
                    timeout=60,
                )
                if result.returncode != 0:
                    raise DirectWardenError(
                        f"could not discard incomplete image mount: "
                        f"{result.stderr or result.stdout}"
                    )
            elif mounted.returncode not in {1, 32}:
                raise DirectWardenError(
                    f"could not inspect incomplete image mount: "
                    f"{mounted.stderr or mounted.stdout}"
                )
        shutil.rmtree(target)

    def _ensure_overlay2_mount(
        self,
        rootfs: Path,
        layers: tuple[Path, ...],
    ) -> None:
        mounted = self.runner.run(
            (self.mountpoint_binary, "--quiet", str(rootfs)),
            timeout=60,
        )
        if mounted.returncode == 0:
            return
        if mounted.returncode not in {1, 32}:
            raise DirectWardenError(
                f"could not inspect overlay2 image mount: "
                f"{mounted.stderr or mounted.stdout}"
            )
        self._mount_overlay2(rootfs, layers)

    def _mount_overlay2(self, rootfs: Path, layers: tuple[Path, ...]) -> None:
        with self._operation_guard:
            self._active_exports += 1
        try:
            if len(layers) == 1:
                result = self.runner.run(
                    (
                        self.mount_binary,
                        "--bind",
                        str(layers[0]),
                        str(rootfs),
                    ),
                    timeout=60,
                )
                if result.returncode == 0:
                    readonly = self.runner.run(
                        (
                            self.mount_binary,
                            "-o",
                            "remount,bind,ro",
                            str(rootfs),
                        ),
                        timeout=60,
                    )
                    if readonly.returncode != 0:
                        cleanup = self.runner.run(
                            (self.umount_binary, str(rootfs)),
                            timeout=60,
                        )
                        if cleanup.returncode != 0:
                            raise DirectWardenError(
                                "single-layer image remount failed and its "
                                "bind mount could not be released: "
                                f"{cleanup.stderr or cleanup.stdout}"
                            )
                        result = readonly
            else:
                result = self.runner.run(
                    (
                        self.mount_binary,
                        "-t",
                        "overlay",
                        "overlay",
                        "-o",
                        "ro,lowerdir=" + ":".join(str(item) for item in layers),
                        str(rootfs),
                    ),
                    timeout=60,
                )
            if result.returncode != 0:
                raise DirectWardenError(
                    f"overlay2 image mount failed: {result.stderr or result.stdout}"
                )
        finally:
            with self._operation_guard:
                self._active_exports -= 1

    @staticmethod
    def _rootfs_identity(image_id: str) -> str:
        # The mounted layer view and the former Docker-exported directory have
        # identical OCI filesystem semantics. Preserve the identity namespace
        # so parked sandboxes can migrate across a rolling upgrade.
        return _sha256(
            b"ucloud-docker-export-rootfs-v1\0" + image_id.encode("ascii")
        )


@dataclass(frozen=True)
class OverlayRootfsLease:
    sandbox: DirectSandbox
    image: MaterializedRootfs
    writable: Path
    upper: Path
    work: Path
    merged: Path
    writable_owned_by_manager: bool


class OverlayRootfsManager:
    """Create per-sandbox overlays over a shared immutable Docker image view."""

    def __init__(
        self,
        image_store: DockerRootfsStore,
        *,
        writable_root: Path,
        bundle_root: Path,
        runner: CommandRunner | None = None,
        mount_binary: str = "mount",
        mountpoint_binary: str = "mountpoint",
        umount_binary: str = "umount",
        require_precreated_writable: bool = False,
    ) -> None:
        self.image_store = image_store
        self.writable_root = writable_root
        self.bundle_root = bundle_root
        self.runner = runner or SubprocessCommandRunner()
        self.mount_binary = mount_binary
        self.mountpoint_binary = mountpoint_binary
        self.umount_binary = umount_binary
        self.require_precreated_writable = bool(require_precreated_writable)
        for path in (writable_root, bundle_root):
            if not path.is_absolute():
                raise ValueError("overlay roots must be absolute")
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            DockerRootfsStore._require_private_directory(path)

    def prepare(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        image_ref: str,
        config_template: dict[str, Any],
        spec_sha256: str | None = None,
        imported_parked: bool = False,
    ) -> OverlayRootfsLease:
        if not _SAFE_ID.fullmatch(sandbox_id) or sandbox_generation < 0:
            raise ValueError("sandbox incarnation is invalid")
        image = self.image_store.materialize(image_ref)
        incarnation = f"{sandbox_id}.sandbox-{sandbox_generation}"
        writable = self.writable_root / incarnation
        bundle = self.bundle_root / incarnation
        upper = writable / "upper"
        work = writable / "work"
        merged = bundle / "rootfs"
        if bundle.exists():
            raise DirectWardenError("overlay sandbox incarnation already exists")
        writable_owned_by_manager = not self.require_precreated_writable
        if self.require_precreated_writable:
            if not writable.exists():
                raise DirectWardenError(
                    "quota-owned writable incarnation was not prepared"
                )
            DockerRootfsStore._require_private_directory(writable)
            existing_names = {item.name for item in writable.iterdir()}
            if imported_parked:
                generations = {
                    name
                    for name in existing_names
                    if re.fullmatch(r"hibernate-[1-9][0-9]*", name)
                }
                if (
                    "upper" not in existing_names
                    or len(generations) != 1
                    or existing_names != {"upper", *generations}
                ):
                    raise DirectWardenError(
                        "imported writable incarnation has an invalid layout"
                    )
                DockerRootfsStore._require_real_directory(upper)
                for generation in generations:
                    DockerRootfsStore._require_private_directory(
                        writable / generation
                    )
            elif existing_names:
                raise DirectWardenError(
                    "quota-owned writable incarnation is not empty"
                )
        elif writable.exists():
            raise DirectWardenError("overlay sandbox incarnation already exists")
        if writable_owned_by_manager:
            writable.mkdir(mode=0o700)
        bundle.mkdir(mode=0o700)
        for path in (upper, work, merged):
            if path == upper and imported_parked:
                continue
            path.mkdir(mode=0o700)
        # Overlayfs exposes the upper directory inode as the mounted root.
        # Keeping it private would make every non-root OCI user unable to
        # traverse "/", regardless of the image root's permissions.
        image_root = image.rootfs.stat()
        upper_info = upper.stat()
        if (
            upper_info.st_uid != image_root.st_uid
            or upper_info.st_gid != image_root.st_gid
        ):
            os.chown(upper, image_root.st_uid, image_root.st_gid)
        os.chmod(upper, image_root.st_mode & 0o7777)
        mounted = False
        try:
            result = self.runner.run(
                (
                    self.mount_binary,
                    "-t",
                    "overlay",
                    "overlay",
                    "-o",
                    f"lowerdir={image.rootfs},upperdir={upper},workdir={work}",
                    str(merged),
                ),
                timeout=60,
            )
            if result.returncode != 0:
                raise DirectWardenError(
                    f"overlay mount failed: {result.stderr or result.stdout}"
                )
            mounted = True
            memory_directory = incarnation
            config = json.loads(json.dumps(config_template))
            config.setdefault("root", {})["path"] = "rootfs"
            config["root"].setdefault("readonly", False)
            config.setdefault("annotations", {})[
                "dev.gvisor.internal.application-memory-directory"
            ] = memory_directory
            config_path = bundle / "config.json"
            DockerRootfsStore._atomic_write(
                config_path,
                json.dumps(config, indent=2, sort_keys=True).encode("utf-8") + b"\n",
            )
            DockerRootfsStore._atomic_write(
                bundle / _OVERLAY_METADATA,
                _canonical_json(
                    {
                        "image_id": image.image_id,
                        "lowerdir": str(image.rootfs),
                        "rootfs_identity_sha256": image.rootfs_identity_sha256,
                        "schema": _OVERLAY_METADATA_SCHEMA,
                    }
                )
                + b"\n",
            )
            DockerRootfsStore._fsync_directory(bundle)
        except Exception as exc:
            if mounted:
                cleanup = self.runner.run(
                    (self.umount_binary, str(merged)),
                    timeout=60,
                )
                if cleanup.returncode != 0:
                    raise DirectWardenError(
                        "overlay preparation failed and its mount could not be "
                        f"released: {cleanup.stderr or cleanup.stdout}"
                    ) from exc
            shutil.rmtree(bundle, ignore_errors=True)
            if writable_owned_by_manager:
                shutil.rmtree(writable, ignore_errors=True)
            else:
                shutil.rmtree(upper, ignore_errors=True)
                shutil.rmtree(work, ignore_errors=True)
            raise
        container_id = hashlib.sha256(
            f"{sandbox_id}:{sandbox_generation}".encode("utf-8")
        ).hexdigest()
        sandbox = DirectSandbox(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            container_id=container_id,
            spec_sha256=spec_sha256 or _sha256(_canonical_json(config)),
            rootfs_sha256=image.rootfs_identity_sha256,
            bundle=bundle,
            memory_directory=memory_directory,
        )
        return OverlayRootfsLease(
            sandbox=sandbox,
            image=image,
            writable=writable,
            upper=upper,
            work=work,
            merged=merged,
            writable_owned_by_manager=writable_owned_by_manager,
        )

    def release(self, lease: OverlayRootfsLease) -> None:
        result = self.runner.run(
            (self.umount_binary, str(lease.merged)),
            timeout=60,
        )
        if result.returncode != 0:
            raise DirectWardenError(
                f"overlay unmount failed: {result.stderr or result.stdout}"
            )
        shutil.rmtree(lease.sandbox.bundle)
        if lease.writable_owned_by_manager:
            shutil.rmtree(lease.writable)

    def release_sandbox(self, sandbox: DirectSandbox) -> None:
        """Release a registered overlay without depending on in-memory lease state."""
        incarnation = f"{sandbox.sandbox_id}.sandbox-{sandbox.sandbox_generation}"
        expected_bundle = self.bundle_root / incarnation
        if sandbox.bundle != expected_bundle:
            raise DirectWardenError("registered sandbox bundle escaped overlay root")
        merged = expected_bundle / "rootfs"
        if expected_bundle.exists():
            self._unmount_if_mounted(merged)
            shutil.rmtree(expected_bundle)
        writable = self.writable_root / incarnation
        if not self.require_precreated_writable and writable.exists():
            shutil.rmtree(writable)

    def park_sandbox(self, sandbox: DirectSandbox) -> None:
        """Detach the nested overlay while preserving its durable bundle."""
        bundle, writable, merged = self._sandbox_paths(sandbox)
        if bundle.exists():
            self._unmount_if_mounted(merged)
        work = writable / "work"
        if work.exists():
            DockerRootfsStore._require_real_directory(work)
            shutil.rmtree(work)

    def resume_sandbox(self, sandbox: DirectSandbox) -> None:
        """Reconstruct the overlay mount after its writable volume is mounted."""
        bundle, writable, merged = self._sandbox_paths(sandbox)
        DockerRootfsStore._require_private_directory(bundle)
        DockerRootfsStore._require_private_directory(writable)
        upper = writable / "upper"
        work = writable / "work"
        DockerRootfsStore._require_real_directory(upper)
        work.mkdir(mode=0o700, exist_ok=True)
        DockerRootfsStore._require_private_directory(work)
        DockerRootfsStore._require_real_directory(merged)
        mounted = self.runner.run(
            (self.mountpoint_binary, "--quiet", str(merged)),
            timeout=60,
        )
        if mounted.returncode == 0:
            return
        if mounted.returncode not in {1, 32}:
            raise DirectWardenError(
                f"could not inspect overlay mount: {mounted.stderr or mounted.stdout}"
            )
        metadata_path = bundle / _OVERLAY_METADATA
        try:
            metadata = json.loads(metadata_path.read_text(encoding="ascii"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DirectWardenError(
                "overlay bundle metadata is invalid"
            ) from exc
        if (
            not isinstance(metadata, dict)
            or set(metadata)
            != {
                "image_id",
                "lowerdir",
                "rootfs_identity_sha256",
                "schema",
            }
            or metadata.get("schema") != _OVERLAY_METADATA_SCHEMA
            or metadata.get("rootfs_identity_sha256") != sandbox.rootfs_sha256
        ):
            raise DirectWardenError("overlay bundle metadata changed")
        image = self.image_store.materialize(str(metadata["image_id"]))
        if (
            image.rootfs_identity_sha256 != sandbox.rootfs_sha256
            or image.image_id != metadata["image_id"]
        ):
            raise DirectWardenError("overlay image identity changed during remount")
        try:
            lower = Path(str(metadata["lowerdir"])).resolve(strict=True)
            lower.relative_to(self.image_store.images.resolve(strict=True))
        except (OSError, ValueError) as exc:
            raise DirectWardenError(
                "overlay lower escaped the immutable image store"
            ) from exc
        DockerRootfsStore._require_real_directory(lower)
        if lower != image.rootfs.resolve(strict=True):
            raise DirectWardenError("overlay lower changed during remount")
        result = self.runner.run(
            (
                self.mount_binary,
                "-t",
                "overlay",
                "overlay",
                "-o",
                f"lowerdir={lower},upperdir={upper},workdir={work}",
                str(merged),
            ),
            timeout=60,
        )
        if result.returncode != 0:
            raise DirectWardenError(
                f"overlay remount failed: {result.stderr or result.stdout}"
            )

    def discard_unregistered(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
    ) -> None:
        """Remove overlay state from a create that crashed before registration."""
        if not _SAFE_ID.fullmatch(sandbox_id) or sandbox_generation < 0:
            raise ValueError("sandbox incarnation is invalid")
        incarnation = f"{sandbox_id}.sandbox-{sandbox_generation}"
        bundle = self.bundle_root / incarnation
        if bundle.exists():
            self._unmount_if_mounted(bundle / "rootfs")
            shutil.rmtree(bundle)
        writable = self.writable_root / incarnation
        if writable.exists():
            if self.require_precreated_writable:
                for name in ("upper", "work"):
                    path = writable / name
                    if path.exists():
                        shutil.rmtree(path)
            else:
                shutil.rmtree(writable)

    def _unmount_if_mounted(self, path: Path) -> None:
        mounted = self.runner.run(
            (self.mountpoint_binary, "--quiet", str(path)),
            timeout=60,
        )
        if mounted.returncode in {1, 32}:
            return
        if mounted.returncode != 0:
            raise DirectWardenError(
                f"could not inspect overlay mount: {mounted.stderr or mounted.stdout}"
            )
        result = self.runner.run(
            (self.umount_binary, str(path)),
            timeout=60,
        )
        if result.returncode != 0:
            raise DirectWardenError(
                f"overlay unmount failed: {result.stderr or result.stdout}"
            )

    def _sandbox_paths(
        self,
        sandbox: DirectSandbox,
    ) -> tuple[Path, Path, Path]:
        incarnation = (
            f"{sandbox.sandbox_id}.sandbox-{sandbox.sandbox_generation}"
        )
        bundle = self.bundle_root / incarnation
        if sandbox.bundle != bundle:
            raise DirectWardenError("registered sandbox bundle escaped overlay root")
        writable = self.writable_root / incarnation
        return bundle, writable, bundle / "rootfs"
