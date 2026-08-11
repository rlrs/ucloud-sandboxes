from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from threading import BoundedSemaphore, Lock
from typing import Any, Callable, Iterable, Iterator

from .direct_warden import (
    CommandRunner,
    DirectSandbox,
    DirectWardenError,
    SubprocessCommandRunner,
)


_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
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


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _require_real_directory(path: Path) -> None:
    if not path.is_dir() or path.is_symlink():
        raise DirectWardenError("rootfs path must be a real directory")


def _require_private_directory(path: Path) -> None:
    _require_real_directory(path)
    info = path.lstat()
    if info.st_uid != os.geteuid() or info.st_mode & 0o022:
        raise DirectWardenError("rootfs store directory must be owned and private")


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

        def optional_string(name: str) -> str:
            value = raw.get(name)
            if value is None:
                return ""
            if not isinstance(value, str):
                raise ValueError(f"Docker image Config.{name} is invalid")
            return value

        return cls(
            entrypoint=string_tuple("Entrypoint"),
            command=string_tuple("Cmd"),
            env=string_tuple("Env"),
            working_dir=optional_string("WorkingDir"),
            user=optional_string("User"),
        )

@dataclass(frozen=True)
class MaterializedRootfs:
    image_ref: str
    image_id: str
    rootfs_identity_sha256: str
    rootfs: Path
    image_config: DockerImageConfig


class DockerOverlay2RootfsStore:
    """Mount Docker's immutable overlay2 layers without flattening the image.

    Docker remains the OCI image owner, but it never owns a sandbox task. A
    digest-specific local tag leases every image used by the runtime so Docker
    pruning cannot remove layers below a live or parked sandbox.
    """

    PIN_REPOSITORY = "ucloud-sandbox-rootfs-cache"
    MAX_CONCURRENT_OPERATIONS = 32
    COMPLETE = "COMPLETE"

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
        max_concurrent_operations: int = MAX_CONCURRENT_OPERATIONS,
    ) -> None:
        if not root.is_absolute():
            raise ValueError("rootfs store root must be absolute")
        if docker_root is not None and not docker_root.is_absolute():
            raise ValueError("Docker data root must be absolute")
        if max_concurrent_operations < 1:
            raise ValueError("rootfs materialization concurrency must be positive")
        self.root = root
        self.runner = runner or SubprocessCommandRunner()
        self.docker_binary = docker_binary
        self._operation_guard = Lock()
        self._active_operations = 0
        self._waiting_operations = 0
        self.max_concurrent_operations = int(max_concurrent_operations)
        self._operation_slots = BoundedSemaphore(self.max_concurrent_operations)
        self._configured_docker_root = docker_root
        self._resolved_docker_root: Path | None = None
        self._docker_root_guard = Lock()
        self._driver_validated = False
        self.mount_binary = mount_binary
        self.mountpoint_binary = mountpoint_binary
        self.umount_binary = umount_binary
        self.images = root / "images"
        self.locks = root / "locks"
        for path in (root, self.images, self.locks):
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            _require_private_directory(path)

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
    def _locked(self, digest: str) -> Iterator[None]:
        descriptor = self._open_digest_lock(digest)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _open_digest_lock(self, digest: str) -> int:
        if not _DIGEST.fullmatch(digest):
            raise ValueError("rootfs cache digest is invalid")
        return os.open(
            self.locks / f"{digest}.lock",
            os.O_RDWR
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )

    @contextmanager
    def _operation_slot(self) -> Iterator[None]:
        with self._operation_guard:
            self._waiting_operations += 1
        acquired = False
        active = False
        try:
            self._operation_slots.acquire()
            acquired = True
            with self._operation_guard:
                self._waiting_operations -= 1
                self._active_operations += 1
                active = True
            yield
        finally:
            with self._operation_guard:
                if active:
                    self._active_operations -= 1
                else:
                    self._waiting_operations -= 1
            if acquired:
                self._operation_slots.release()

    def _acquire_digest_lock(self, descriptor: int, operation: int) -> None:
        """Wait for a digest lease without consuming a materialization slot."""

        with self._operation_guard:
            self._waiting_operations += 1
        try:
            fcntl.flock(descriptor, operation)
        finally:
            with self._operation_guard:
                self._waiting_operations -= 1

    def operation_snapshot(self) -> dict[str, int]:
        with self._operation_guard:
            return {
                "active_operations": self._active_operations,
                "waiting_operations": self._waiting_operations,
                "max_concurrent_operations": self.max_concurrent_operations,
            }

    def warm(self, image_ref: str) -> None:
        """Populate the cache without exposing an unleased rootfs handle."""

        with self.operation_lease(image_ref) as image:
            del image

    @contextmanager
    def operation_lease(self, image_ref: str) -> Iterator[MaterializedRootfs]:
        """Materialize an image and protect its digest until durable commit.

        The shared flock is process-crash safe. GC takes the same lock
        exclusively and rechecks the registry after any in-flight provisioner
        has either committed its durable image root or released this lease.
        """

        image_ref = str(image_ref).strip()
        if not image_ref or "\0" in image_ref:
            raise ValueError("image_ref is invalid")
        descriptor: int | None = None
        with self._operation_slot():
            image_id, image_config, layers = self._inspect_overlay2(image_ref)
        digest = image_id[7:]
        descriptor = self._open_digest_lock(digest)
        image: MaterializedRootfs | None = None
        lock_held = False
        try:
            while True:
                # Digest contention must not consume a global materialization
                # slot: many requests for one cold image otherwise starve
                # unrelated images by filling the semaphore in flock(2).
                self._acquire_digest_lock(descriptor, fcntl.LOCK_SH)
                lock_held = True
                with self._operation_slot():
                    image = self._load_overlay2_complete(
                        digest,
                        image_ref=image_ref,
                        image_config=image_config,
                    )
                    mounted = image is not None and self._overlay2_mount_present(
                        image.rootfs
                    )
                if image is not None and mounted:
                    break

                fcntl.flock(descriptor, fcntl.LOCK_UN)
                lock_held = False
                self._acquire_digest_lock(descriptor, fcntl.LOCK_EX)
                lock_held = True
                with self._operation_slot():
                    image = self._materialize_locked(
                        image_ref=image_ref,
                        image_id=image_id,
                        image_config=image_config,
                        layers=layers,
                    )
                # Release EX instead of converting it: flock conversion can
                # briefly drop the lock, and keeping EX through provisioning
                # would serialize every caller of this digest. The next loop
                # acquires SH and revalidates; if GC won this gap, the missing
                # cache entry is rematerialized before anything is yielded.
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                lock_held = False
        except BaseException:
            if lock_held:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)
            descriptor = None
            raise
        assert descriptor is not None and image is not None
        try:
            yield image
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _materialize_locked(
        self,
        *,
        image_ref: str,
        image_id: str,
        image_config: DockerImageConfig,
        layers: tuple[Path, ...],
    ) -> MaterializedRootfs:
        digest = image_id[7:]
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
        rootfs = target / "rootfs"
        identity = self._rootfs_identity(image_id)
        mounted = False
        try:
            target.mkdir(mode=0o700)
            rootfs.mkdir(mode=0o755)
            self._mount_overlay2(rootfs, layers)
            mounted = True
            marker = {
                "image_id": image_id,
                "rootfs_identity_sha256": identity,
                "schema": _OVERLAY2_ROOTFS_SCHEMA,
                "store": "docker-overlay2",
            }
            _atomic_write(
                target / self.COMPLETE,
                _canonical_json(marker) + b"\n",
            )
            _fsync_directory(target)
            _fsync_directory(self.images)
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
            try:
                self._unpin_image(image_id)
            except Exception as cleanup_exc:
                raise DirectWardenError(
                    "overlay2 image publication failed and its private pin "
                    "could not be released"
                ) from cleanup_exc
            raise
        return MaterializedRootfs(
            image_ref=image_ref,
            image_id=image_id,
            rootfs_identity_sha256=identity,
            rootfs=rootfs,
            image_config=image_config,
        )

    def reconcile_images(
        self,
        referenced_image_ids: Iterable[str],
        *,
        is_referenced: Callable[[str], bool],
    ) -> dict[str, int]:
        """Reconcile durable roots and collect every unreferenced cache entry."""

        self._validate_docker_driver()
        self._docker_overlay2_root()
        roots = frozenset(str(image_id) for image_id in referenced_image_ids)
        if any(
            not image_id.startswith("sha256:")
            or not _DIGEST.fullmatch(image_id[7:])
            for image_id in roots
        ):
            raise ValueError("referenced rootfs image id is invalid")
        pins = self._list_pinned_images()
        retained = 0
        collected = 0
        for target in sorted(self.images.iterdir(), key=lambda item: item.name):
            if target.name.startswith("."):
                continue
            if not _DIGEST.fullmatch(target.name):
                raise DirectWardenError(
                    "overlay2 image cache contains an invalid entry"
                )
            image_id = f"sha256:{target.name}"
            with self._locked(target.name):
                if not target.exists():
                    continue
                _require_private_directory(target)
                try:
                    marker = json.loads(
                        (target / self.COMPLETE).read_text(encoding="ascii")
                    )
                except FileNotFoundError:
                    if image_id in roots or is_referenced(image_id):
                        raise DirectWardenError(
                            "referenced overlay2 rootfs publication is incomplete"
                        )
                    self._discard_overlay2_target(target)
                    if target.name in pins:
                        self._unpin_image(image_id)
                        pins.pop(target.name)
                    collected += 1
                    continue
                except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise DirectWardenError("overlay2 rootfs marker is invalid") from exc
                if marker != {
                    "image_id": image_id,
                    "rootfs_identity_sha256": self._rootfs_identity(image_id),
                    "schema": _OVERLAY2_ROOTFS_SCHEMA,
                    "store": "docker-overlay2",
                }:
                    raise DirectWardenError("overlay2 rootfs marker identity is invalid")
                rooted = image_id in roots or is_referenced(image_id)
                if rooted:
                    if target.name not in pins:
                        self._pin_image(image_id)
                    retained += 1
                    continue
                # Keep the private tag until the cache mount and directory are
                # safely gone. A crash can then leak a harmless tag, never leave
                # a cache mount backed by an image Docker may prune.
                self._discard_overlay2_target(target)
                if target.name in pins:
                    self._unpin_image(image_id)
                    pins.pop(target.name)
                collected += 1

        orphan_pins_removed = 0
        for digest, image_id in pins.items():
            target = self.images / digest
            if target.exists():
                continue
            with self._locked(digest):
                if target.exists():
                    continue
                if image_id in roots or is_referenced(image_id):
                    raise DirectWardenError(
                        "referenced Docker image pin has no rootfs cache entry"
                    )
                self._unpin_image(image_id)
                orphan_pins_removed += 1

        missing = sorted(
            image_id for image_id in roots if not (self.images / image_id[7:]).exists()
        )
        if missing:
            raise DirectWardenError("direct registry references a missing rootfs cache")
        return {
            "collected": collected,
            "orphan_pins_removed": orphan_pins_removed,
            "retained": retained,
        }

    def collect_image(
        self,
        image_id: str,
        *,
        is_referenced: Callable[[str], bool],
    ) -> bool:
        """Collect one deleted registration's digest in constant work."""

        if not image_id.startswith("sha256:") or not _DIGEST.fullmatch(image_id[7:]):
            raise ValueError("rootfs image id is invalid")
        digest = image_id[7:]
        target = self.images / digest
        with self._locked(digest):
            pins = self._list_pinned_images(digest)
            rooted = is_referenced(image_id)
            if not target.exists():
                if rooted:
                    raise DirectWardenError(
                        "referenced Docker image pin has no rootfs cache entry"
                    )
                if digest in pins:
                    self._unpin_image(image_id)
                    return True
                return False
            _require_private_directory(target)
            try:
                marker = json.loads(
                    (target / self.COMPLETE).read_text(encoding="ascii")
                )
            except FileNotFoundError:
                if rooted:
                    raise DirectWardenError(
                        "referenced overlay2 rootfs publication is incomplete"
                    )
                self._discard_overlay2_target(target)
                if digest in pins:
                    self._unpin_image(image_id)
                return True
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise DirectWardenError("overlay2 rootfs marker is invalid") from exc
            if marker != {
                "image_id": image_id,
                "rootfs_identity_sha256": self._rootfs_identity(image_id),
                "schema": _OVERLAY2_ROOTFS_SCHEMA,
                "store": "docker-overlay2",
            }:
                raise DirectWardenError("overlay2 rootfs marker identity is invalid")
            if rooted:
                if digest not in pins:
                    self._pin_image(image_id)
                return False
            self._discard_overlay2_target(target)
            if digest in pins:
                self._unpin_image(image_id)
            return True

    def _pin_image(self, image_id: str) -> None:
        self._checked(
            self.docker_binary,
            "image",
            "tag",
            image_id,
            f"{self.PIN_REPOSITORY}:{image_id[7:]}",
        )

    def _unpin_image(self, image_id: str) -> None:
        self._checked(
            self.docker_binary,
            "image",
            "rm",
            f"{self.PIN_REPOSITORY}:{image_id[7:]}",
        )

    def _list_pinned_images(self, only_digest: str | None = None) -> dict[str, str]:
        if only_digest is not None and not _DIGEST.fullmatch(only_digest):
            raise ValueError("rootfs cache digest is invalid")
        reference = self.PIN_REPOSITORY + ":" + (only_digest or "*")
        output = self._checked(
            self.docker_binary,
            "image",
            "ls",
            "--no-trunc",
            "--filter",
            f"reference={reference}",
            "--format={{.Repository}} {{.Tag}} {{.ID}}",
        )
        pins: dict[str, str] = {}
        for line in output.splitlines():
            fields = line.split()
            if len(fields) != 3:
                raise DirectWardenError("Docker image pin inventory is invalid")
            repository, tag_digest, image_id = fields
            if (
                repository != self.PIN_REPOSITORY
                or not _DIGEST.fullmatch(tag_digest)
                or image_id != f"sha256:{tag_digest}"
                or tag_digest in pins
            ):
                raise DirectWardenError("Docker image pin inventory is invalid")
            pins[tag_digest] = image_id
        if only_digest is not None and any(item != only_digest for item in pins):
            raise DirectWardenError("Docker image pin inventory is invalid")
        return pins

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
        _require_real_directory(resolved)
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
            _require_real_directory(resolved)
            info = resolved.stat()
            if info.st_uid != os.geteuid() or info.st_mode & 0o022:
                raise DirectWardenError("Docker data root is not safely owned")
            self._resolved_docker_root = resolved
            overlay2 = resolved / "overlay2"
            _require_real_directory(overlay2)
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
        _require_private_directory(target)
        rootfs = target / "rootfs"
        try:
            marker = json.loads((target / self.COMPLETE).read_text(encoding="ascii"))
        except FileNotFoundError:
            return None
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DirectWardenError("overlay2 rootfs metadata is invalid") from exc
        _require_real_directory(rootfs)
        image_id = f"sha256:{digest}"
        identity = self._rootfs_identity(image_id)
        if (
            marker
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
        if self._overlay2_mount_present(rootfs):
            return
        self._mount_overlay2(rootfs, layers)

    def _overlay2_mount_present(self, rootfs: Path) -> bool:
        mounted = self.runner.run(
            (self.mountpoint_binary, "--quiet", str(rootfs)),
            timeout=60,
        )
        if mounted.returncode == 0:
            return True
        if mounted.returncode not in {1, 32}:
            raise DirectWardenError(
                f"could not inspect overlay2 image mount: "
                f"{mounted.stderr or mounted.stdout}"
            )
        return False

    def _mount_overlay2(self, rootfs: Path, layers: tuple[Path, ...]) -> None:
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

    @staticmethod
    def _rootfs_identity(image_id: str) -> str:
        return _sha256(b"ucloud-overlay2-rootfs-v1\0" + image_id.encode("ascii"))


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
        image_store: DockerOverlay2RootfsStore,
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
            _require_private_directory(path)

    def prepare(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        image: MaterializedRootfs,
        config_template: dict[str, Any],
        spec_sha256: str | None = None,
        imported_parked: bool = False,
    ) -> OverlayRootfsLease:
        if not _SAFE_ID.fullmatch(sandbox_id) or sandbox_generation < 1:
            raise ValueError("sandbox incarnation is invalid")
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
            _require_private_directory(writable)
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
                _require_real_directory(upper)
                for generation in generations:
                    _require_private_directory(writable / generation)
            elif existing_names:
                raise DirectWardenError("quota-owned writable incarnation is not empty")
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
            _atomic_write(
                config_path,
                json.dumps(config, indent=2, sort_keys=True).encode("utf-8") + b"\n",
            )
            _atomic_write(
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
            _fsync_directory(bundle)
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
            _require_real_directory(work)
            shutil.rmtree(work)

    def resume_sandbox(self, sandbox: DirectSandbox) -> None:
        """Reconstruct the overlay mount after its writable volume is mounted."""
        bundle, writable, merged = self._sandbox_paths(sandbox)
        _require_private_directory(bundle)
        _require_private_directory(writable)
        upper = writable / "upper"
        work = writable / "work"
        _require_real_directory(upper)
        work.mkdir(mode=0o700, exist_ok=True)
        _require_private_directory(work)
        _require_real_directory(merged)
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
            raise DirectWardenError("overlay bundle metadata is invalid") from exc
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
        with self.image_store.operation_lease(str(metadata["image_id"])) as image:
            if (
                image.rootfs_identity_sha256 != sandbox.rootfs_sha256
                or image.image_id != metadata["image_id"]
            ):
                raise DirectWardenError(
                    "overlay image identity changed during remount"
                )
            try:
                lower = Path(str(metadata["lowerdir"])).resolve(strict=True)
                lower.relative_to(self.image_store.images.resolve(strict=True))
            except (OSError, ValueError) as exc:
                raise DirectWardenError(
                    "overlay lower escaped the immutable image store"
                ) from exc
            _require_real_directory(lower)
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
        incarnation = f"{sandbox.sandbox_id}.sandbox-{sandbox.sandbox_generation}"
        bundle = self.bundle_root / incarnation
        if sandbox.bundle != bundle:
            raise DirectWardenError("registered sandbox bundle escaped overlay root")
        writable = self.writable_root / incarnation
        return bundle, writable, bundle / "rootfs"
