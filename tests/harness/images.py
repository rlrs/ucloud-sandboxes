"""Local-directory images in place of Docker's overlay2 store.

``ImageCatalog`` is the fleet's registry: named image directories with an
OCI image config. ``LocalRootfsStore`` is one node's ``ImmutableRootfsStore``:
it copies a catalog image into its own cache on first use (a pull) and hands
the real ``OverlayRootfsManager`` that directory as the immutable lower.
Identity follows the Docker overlay2 ABI, so bundle metadata, checkpoint
fingerprints and remount validation are unchanged production code.
"""

from __future__ import annotations

from contextlib import contextmanager
import dataclasses
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import shutil
import threading
from typing import Callable, Iterable, Iterator, Mapping

from ucloud_sandboxes.direct_warden import DirectWardenError
from ucloud_sandboxes.environment_manifest import DOCKER_OVERLAY2_ABI, EnvironmentManifest
from ucloud_sandboxes.image_rootfs import DockerImageConfig, MaterializedRootfs


@dataclass(frozen=True)
class CatalogImage:
    ref: str
    image_id: str
    rootfs: Path
    config: DockerImageConfig


def _normalize(ref: str) -> str:
    name = ref.rsplit("/", 1)[-1]
    return ref if ":" in name or "@" in ref else ref + ":latest"


class ImageCatalog:
    """Refs live only in ``refs.json``, so every process sees each add,
    including a ref re-added with new content."""

    def __init__(self, root: Path) -> None:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.root = root
        self._guard = threading.Lock()

    def _index(self) -> dict[str, CatalogImage]:
        try:
            raw = json.loads((self.root / "refs.json").read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        return {
            key: CatalogImage(item["ref"], item["image_id"], Path(item["rootfs"]), DockerImageConfig(
                **{name: tuple(value) if isinstance(value, list) else value
                   for name, value in item["config"].items()}))
            for key, item in raw.items()
        }

    def add(
        self,
        ref: str,
        files: Mapping[str, bytes | str],
        *,
        command: tuple[str, ...] = ("sleep", "infinity"),
        env: tuple[str, ...] = ("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",),
        working_dir: str = "",
        user: str = "0",
    ) -> CatalogImage:
        """Publish an image whose rootfs holds exactly ``files``.

        Paths are relative to /. A trailing ``/`` names an empty directory.
        """
        config = DockerImageConfig(command=command, env=env, working_dir=working_dir, user=user)
        manifest = {
            "config": {"cmd": list(command), "env": list(env), "user": user, "workdir": working_dir},
            "files": {
                path: hashlib.sha256(
                    data.encode("utf-8") if isinstance(data, str) else data
                ).hexdigest()
                for path, data in sorted(files.items())
            },
        }
        digest = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode("ascii")).hexdigest()
        rootfs = self.root / digest / "rootfs"
        if not rootfs.exists():
            staging = self.root / f".{digest}.staging"
            shutil.rmtree(staging, ignore_errors=True)
            (staging / "rootfs").mkdir(mode=0o755, parents=True)
            for path, data in files.items():
                target = staging / "rootfs" / path.strip("/")
                if path.endswith("/"):
                    target.mkdir(mode=0o755, parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
                target.write_bytes(data.encode("utf-8") if isinstance(data, str) else data)
            staging.rename(self.root / digest)
        image = CatalogImage(ref, "sha256:" + digest, rootfs, config)
        with self._guard:
            images = {**self._index(), _normalize(ref): image}
            staging = self.root / ".refs.json"
            staging.write_text(json.dumps({
                key: {"ref": item.ref, "image_id": item.image_id, "rootfs": str(item.rootfs),
                      "config": dataclasses.asdict(item.config)}
                for key, item in images.items()
            }), encoding="utf-8")
            staging.replace(self.root / "refs.json")
        return image

    def resolve(self, ref: str) -> CatalogImage:
        images = self._index()
        image = images.get(_normalize(ref))
        if image is None:
            image = next((item for item in images.values() if item.image_id == ref), None)
        if image is None:
            raise DirectWardenError(f"image is not in the harness catalog: {ref}")
        return image


class LocalRootfsStore:
    backend_abi = DOCKER_OVERLAY2_ABI

    def __init__(self, root: Path, catalog: ImageCatalog) -> None:
        self.root = root
        self.images = root / "images"
        self.images.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.catalog = catalog
        self._guard = threading.Lock()
        self.pulled: list[str] = []

    def _materialize(self, image_ref: str) -> MaterializedRootfs:
        image = self.catalog.resolve(image_ref)
        digest = image.image_id.removeprefix("sha256:")
        cached = self.images / digest
        with self._guard:
            if not (cached / "COMPLETE").exists():
                shutil.rmtree(cached, ignore_errors=True)
                cached.mkdir(mode=0o700)
                shutil.copytree(image.rootfs, cached / "rootfs", symlinks=True)
                (cached / "COMPLETE").write_text(image.image_id + "\n", encoding="ascii")
                self.pulled.append(image.image_id)
        return MaterializedRootfs(
            image_ref=image_ref,
            image_id=image.image_id,
            rootfs_identity_sha256=self.rootfs_identity(image.image_id),
            rootfs=cached / "rootfs",
            image_config=image.config,
        )

    @staticmethod
    def rootfs_identity(image_id: str) -> str:
        return EnvironmentManifest(base=image_id).rootfs_fingerprint(DOCKER_OVERLAY2_ABI)

    @contextmanager
    def operation_lease(self, image_ref: str, environment_root=None) -> Iterator[MaterializedRootfs]:
        yield self._materialize(image_ref)

    @contextmanager
    def mounted_rootfs_lease(self, image_id: str, *, rootfs_identity_sha256: str) -> Iterator[Path]:
        if rootfs_identity_sha256 != self.rootfs_identity(image_id):
            raise DirectWardenError("overlay image identity changed during remount")
        cached = self.images / image_id.removeprefix("sha256:")
        if not (cached / "COMPLETE").exists():
            # A collected cache entry is re-pulled, as Docker would.
            self._materialize(image_id)
        yield cached / "rootfs"

    def warm(self, image_ref: str) -> None:
        self._materialize(image_ref)

    def collect_image(self, image_id: str, *, is_referenced: Callable[[str], bool]) -> bool:
        with self._guard:
            if is_referenced(image_id):
                return False
            shutil.rmtree(self.images / image_id.removeprefix("sha256:"), ignore_errors=True)
            return True

    def reconcile_images(
        self, image_ids: Iterable[str], *, is_referenced: Callable[[str], bool]
    ) -> dict[str, int]:
        del image_ids
        collected = 0
        for cached in list(self.images.iterdir()):
            collected += self.collect_image("sha256:" + cached.name, is_referenced=is_referenced)
        return {"collected_images": collected}

    def operation_snapshot(self) -> dict[str, int]:
        return {"active_operations": 0, "waiting_operations": 0, "max_concurrent_operations": 4}
