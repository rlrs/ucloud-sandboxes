from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import threading
from typing import Any, Protocol
from urllib.parse import quote

from .managed_registry import RegistryClient
from .storage_native import StorageNativeLayer


OCI_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
OCI_CONFIG_MEDIA_TYPE = "application/vnd.ucloud.sandbox.snapshot.v1+json"
OCI_OVERLAYBD_LAYER_MEDIA_TYPE = (
    "application/vnd.ucloud.overlaybd.layer.v1+lsmt"
)
SNAPSHOT_SCHEMA = "ucloud-storage-native-snapshot-v1"


class DenseLayerExporter(Protocol):
    def export_dense_layer(
        self,
        *,
        source_layer_path: Path,
        stream_socket_path: Path,
    ) -> StorageNativeLayer: ...


@dataclass(frozen=True)
class PublishedStorageLayer:
    digest: str
    size: int

    def to_dict(self) -> dict[str, Any]:
        return {"digest": self.digest, "size": self.size}

    @classmethod
    def from_dict(cls, raw: object) -> "PublishedStorageLayer":
        if not isinstance(raw, dict):
            raise ValueError("published layer must be an object")
        digest = str(raw.get("digest") or "")
        size = raw.get("size")
        if (
            not _is_digest(digest)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size <= 0
        ):
            raise ValueError("published layer descriptor is invalid")
        return cls(digest=digest, size=size)


@dataclass(frozen=True)
class StorageSnapshotPublication:
    manifest_digest: str
    tag: str
    repository: str
    repo_blob_url: str
    virtual_size: int
    layers: tuple[PublishedStorageLayer, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_digest": self.manifest_digest,
            "tag": self.tag,
            "repository": self.repository,
            "repo_blob_url": self.repo_blob_url,
            "virtual_size": self.virtual_size,
            "layers": [layer.to_dict() for layer in self.layers],
        }

    @classmethod
    def from_dict(cls, raw: object) -> "StorageSnapshotPublication":
        if not isinstance(raw, dict):
            raise ValueError("snapshot publication must be an object")
        layers_raw = raw.get("layers")
        if not isinstance(layers_raw, list) or not layers_raw:
            raise ValueError("snapshot publication has no layers")
        manifest_digest = str(raw.get("manifest_digest") or "")
        tag = str(raw.get("tag") or "")
        repository = str(raw.get("repository") or "")
        repo_blob_url = str(raw.get("repo_blob_url") or "")
        virtual_size = raw.get("virtual_size")
        if not _is_digest(manifest_digest):
            raise ValueError("snapshot manifest digest is invalid")
        if not tag or len(tag) > 128:
            raise ValueError("snapshot tag is invalid")
        if not repository or ".." in repository.split("/"):
            raise ValueError("snapshot repository is invalid")
        if not repo_blob_url.startswith(("http://", "https://")):
            raise ValueError("snapshot blob URL is invalid")
        if (
            isinstance(virtual_size, bool)
            or not isinstance(virtual_size, int)
            or virtual_size <= 0
        ):
            raise ValueError("snapshot virtual size is invalid")
        return cls(
            manifest_digest=manifest_digest,
            tag=tag,
            repository=repository,
            repo_blob_url=repo_blob_url,
            virtual_size=virtual_size,
            layers=tuple(
                PublishedStorageLayer.from_dict(layer)
                for layer in layers_raw
            ),
        )


class RegistrySnapshotPublisher:
    def __init__(
        self,
        registry: RegistryClient,
        *,
        repository: str,
        stream_socket_root: Path,
        upload_chunk_bytes: int = 8 * 1024 * 1024,
        stream_timeout_seconds: float = 120.0,
        max_concurrent_publications: int = 2,
    ) -> None:
        if not repository or repository.startswith("/") or ".." in repository.split("/"):
            raise ValueError("snapshot repository is invalid")
        if not stream_socket_root.is_absolute():
            raise ValueError("stream socket root must be absolute")
        if upload_chunk_bytes <= 0:
            raise ValueError("upload chunk size must be positive")
        if stream_timeout_seconds <= 0:
            raise ValueError("stream timeout must be positive")
        if max_concurrent_publications <= 0:
            raise ValueError("publication concurrency must be positive")
        self.registry = registry
        self.repository = repository
        self.stream_socket_root = stream_socket_root
        self.upload_chunk_bytes = upload_chunk_bytes
        self.stream_timeout_seconds = stream_timeout_seconds
        self._publication_slots = threading.BoundedSemaphore(
            max_concurrent_publications
        )

    @property
    def repo_blob_url(self) -> str:
        repository = quote(self.repository, safe="/")
        return f"{self.registry.base_url}/v2/{repository}/blobs"

    def publish(
        self,
        *,
        exporter: DenseLayerExporter,
        source_layer_paths: tuple[Path, ...],
        virtual_size: int,
        existing_layers: tuple[PublishedStorageLayer, ...] = (),
    ) -> StorageSnapshotPublication:
        with self._publication_slots:
            return self._publish_locked(
                exporter=exporter,
                source_layer_paths=source_layer_paths,
                virtual_size=virtual_size,
                existing_layers=existing_layers,
            )

    def verify(
        self,
        publication: StorageSnapshotPublication,
    ) -> StorageSnapshotPublication:
        if publication.repository != self.repository:
            raise ValueError("snapshot publication belongs to another repository")
        if publication.repo_blob_url != self.repo_blob_url:
            raise ValueError("snapshot publication blob URL is not configured")
        manifest, headers = self.registry.manifest_document(
            publication.repository,
            publication.manifest_digest,
        )
        response_digest = str(headers.get("Docker-Content-Digest") or "")
        if response_digest and response_digest != publication.manifest_digest:
            raise ValueError("registry returned another snapshot manifest")
        config = manifest.get("config")
        raw_layers = manifest.get("layers")
        if not isinstance(config, dict) or not isinstance(raw_layers, list):
            raise ValueError("snapshot OCI manifest is malformed")
        config_digest = str(config.get("digest") or "")
        config_size = config.get("size")
        if (
            not _is_digest(config_digest)
            or isinstance(config_size, bool)
            or not isinstance(config_size, int)
            or config_size <= 0
        ):
            raise ValueError("snapshot OCI config descriptor is invalid")
        config_payload = self.registry.blob_bytes(
            publication.repository,
            config_digest,
            max_bytes=1024 * 1024,
        )
        if (
            len(config_payload) != config_size
            or f"sha256:{hashlib.sha256(config_payload).hexdigest()}"
            != config_digest
        ):
            raise ValueError("snapshot config content does not match its descriptor")
        try:
            snapshot_config = json.loads(config_payload.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("snapshot config is invalid JSON") from exc
        expected_config = {
            "schema": SNAPSHOT_SCHEMA,
            "virtualSize": publication.virtual_size,
            "layers": [layer.to_dict() for layer in publication.layers],
        }
        if snapshot_config != expected_config:
            raise ValueError("snapshot config does not match the requested publication")
        manifest_layers = tuple(
            PublishedStorageLayer.from_dict(layer)
            for layer in raw_layers
        )
        if manifest_layers != publication.layers:
            raise ValueError("snapshot manifest layers do not match its config")
        return publication

    def _publish_locked(
        self,
        *,
        exporter: DenseLayerExporter,
        source_layer_paths: tuple[Path, ...],
        virtual_size: int,
        existing_layers: tuple[PublishedStorageLayer, ...],
    ) -> StorageSnapshotPublication:
        if virtual_size <= 0:
            raise ValueError("snapshot virtual size must be positive")
        if not source_layer_paths and not existing_layers:
            raise ValueError("snapshot requires at least one sealed layer")
        new_layers = tuple(
            self._publish_dense_layer(exporter, source)
            for source in source_layer_paths
        )
        layers = (*existing_layers, *new_layers)
        config = self._snapshot_config(virtual_size=virtual_size, layers=layers)
        config_digest = self._upload_bytes(config)
        tag = f"ucloud-storage-v1-{config_digest.removeprefix('sha256:')}"
        manifest = self._oci_manifest(
            config_digest=config_digest,
            config_size=len(config),
            layers=layers,
        )
        manifest_digest = self.registry.put_manifest(
            self.repository,
            tag,
            manifest,
            media_type=OCI_MANIFEST_MEDIA_TYPE,
        )
        return StorageSnapshotPublication(
            manifest_digest=manifest_digest,
            tag=tag,
            repository=self.repository,
            repo_blob_url=self.repo_blob_url,
            virtual_size=virtual_size,
            layers=layers,
        )

    def _publish_dense_layer(
        self,
        exporter: DenseLayerExporter,
        source_layer_path: Path,
    ) -> PublishedStorageLayer:
        if not source_layer_path.is_absolute():
            raise ValueError("sealed layer path must be absolute")
        upload_location = self.registry.start_blob_upload(self.repository)
        self.stream_socket_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="dense-",
            dir=self.stream_socket_root,
        ) as raw_dir:
            socket_path = Path(raw_dir) / "stream.sock"
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(str(socket_path))
                socket_path.chmod(0o600)
                listener.listen(1)
                listener.settimeout(self.stream_timeout_seconds)
                result: list[StorageNativeLayer] = []
                failure: list[BaseException] = []

                def export() -> None:
                    try:
                        result.append(
                            exporter.export_dense_layer(
                                source_layer_path=source_layer_path,
                                stream_socket_path=socket_path,
                            )
                        )
                    except BaseException as exc:
                        failure.append(exc)

                thread = threading.Thread(target=export, daemon=True)
                thread.start()
                hasher = hashlib.sha256()
                byte_count = 0
                pending = bytearray()
                try:
                    connection, _ = listener.accept()
                    with connection:
                        connection.settimeout(self.stream_timeout_seconds)
                        while True:
                            chunk = connection.recv(self.upload_chunk_bytes)
                            if not chunk:
                                break
                            hasher.update(chunk)
                            byte_count += len(chunk)
                            pending.extend(chunk)
                            while len(pending) >= self.upload_chunk_bytes:
                                upload_location = self.registry.upload_blob_chunk(
                                    upload_location,
                                    bytes(pending[: self.upload_chunk_bytes]),
                                )
                                del pending[: self.upload_chunk_bytes]
                        if pending:
                            upload_location = self.registry.upload_blob_chunk(
                                upload_location,
                                bytes(pending),
                            )
                finally:
                    thread.join(timeout=self.stream_timeout_seconds)
                if thread.is_alive():
                    raise TimeoutError("dense layer exporter did not finish")
                if failure:
                    raise failure[0]
                if len(result) != 1:
                    raise RuntimeError("dense layer exporter returned no descriptor")

        observed = StorageNativeLayer(
            digest=f"sha256:{hasher.hexdigest()}",
            size=byte_count,
        )
        if observed != result[0]:
            raise ValueError(
                "dense layer stream does not match the backend descriptor"
            )
        self.registry.finish_blob_upload(upload_location, observed.digest)
        if not self.registry.blob_exists(self.repository, observed.digest):
            raise ValueError("registry did not retain the uploaded dense layer")
        return PublishedStorageLayer(digest=observed.digest, size=observed.size)

    def _upload_bytes(self, payload: bytes) -> str:
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        if self.registry.blob_exists(self.repository, digest):
            return digest
        location = self.registry.start_blob_upload(self.repository)
        for offset in range(0, len(payload), self.upload_chunk_bytes):
            location = self.registry.upload_blob_chunk(
                location,
                payload[offset : offset + self.upload_chunk_bytes],
            )
        self.registry.finish_blob_upload(location, digest)
        if not self.registry.blob_exists(self.repository, digest):
            raise ValueError("registry did not retain the uploaded snapshot config")
        return digest

    @staticmethod
    def _snapshot_config(
        *,
        virtual_size: int,
        layers: tuple[PublishedStorageLayer, ...],
    ) -> bytes:
        return json.dumps(
            {
                "schema": SNAPSHOT_SCHEMA,
                "virtualSize": virtual_size,
                "layers": [layer.to_dict() for layer in layers],
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    @staticmethod
    def _oci_manifest(
        *,
        config_digest: str,
        config_size: int,
        layers: tuple[PublishedStorageLayer, ...],
    ) -> bytes:
        return json.dumps(
            {
                "schemaVersion": 2,
                "mediaType": OCI_MANIFEST_MEDIA_TYPE,
                "config": {
                    "mediaType": OCI_CONFIG_MEDIA_TYPE,
                    "digest": config_digest,
                    "size": config_size,
                },
                "layers": [
                    {
                        "mediaType": OCI_OVERLAYBD_LAYER_MEDIA_TYPE,
                        "digest": layer.digest,
                        "size": layer.size,
                        "annotations": {
                            "containerd.io/snapshot/overlaybd/blob-digest": (
                                layer.digest
                            ),
                            "containerd.io/snapshot/overlaybd/blob-size": str(
                                layer.size
                            ),
                        },
                    }
                    for layer in layers
                ],
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")


def _is_digest(value: str) -> bool:
    return (
        value.startswith("sha256:")
        and len(value) == 71
        and all(character in "0123456789abcdef" for character in value[7:])
    )
