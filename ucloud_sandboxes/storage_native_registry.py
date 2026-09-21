from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import threading
import time
from typing import Any, Callable, Protocol
from urllib.parse import quote

from .managed_registry import RegistryClient
from .storage_native import StorageNativeLayer
from .storage_native_publication import (
    DEFAULT_MAX_CONCURRENT_PUBLICATIONS,
    CompletedLayerUploads,
    PublicationGate,
    local_layer_data_bytes,
    local_layer_identity,
    snapshot_compaction_start,
)
from .telemetry import Telemetry


OCI_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
OCI_CONFIG_MEDIA_TYPE = "application/vnd.ucloud.sandbox.snapshot.v1+json"
OCI_OVERLAYBD_LAYER_MEDIA_TYPE = "application/vnd.ucloud.overlaybd.layer.v1+lsmt"
SNAPSHOT_SCHEMA = "ucloud-storage-native-snapshot-v1"
DEFAULT_COMPACT_AFTER_LAYERS = 8
DEFAULT_COMPACT_AFTER_BYTES = 4 * 1024 * 1024 * 1024


class DenseLayerExporter(Protocol):
    def export_dense_layer(
        self,
        *,
        source_layer_path: Path,
        stream_socket_path: Path,
    ) -> StorageNativeLayer: ...


class SnapshotPublisher(Protocol):
    def publish(
        self,
        *,
        exporter: DenseLayerExporter,
        source_layer_paths: tuple[Path, ...],
        virtual_size: int,
        existing_layers: tuple[PublishedStorageLayer, ...] = (),
        existing_repo_blob_url: str = "",
        global_config_path: Path | None = None,
        check_current: Callable[[], None] | None = None,
    ) -> "StorageSnapshotPublication": ...

    def verify(
        self, publication: "StorageSnapshotPublication"
    ) -> "StorageSnapshotPublication": ...

    def metrics(self) -> dict[str, int]: ...

    def export_compacted_image(
        self,
        *,
        source_image_config: Path,
        global_config: Path,
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
    backend: str = "registry"

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_digest": self.manifest_digest,
            "tag": self.tag,
            "repository": self.repository,
            "repo_blob_url": self.repo_blob_url,
            "virtual_size": self.virtual_size,
            "layers": [layer.to_dict() for layer in self.layers],
            "backend": self.backend,
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
        backend = str(raw.get("backend") or "")
        if not backend:
            backend = "s3" if repo_blob_url.startswith("s3://") else "registry"
        virtual_size = raw.get("virtual_size")
        if not _is_digest(manifest_digest):
            raise ValueError("snapshot manifest digest is invalid")
        if not tag or len(tag) > 128:
            raise ValueError("snapshot tag is invalid")
        if not repository or ".." in repository.split("/"):
            raise ValueError("snapshot repository is invalid")
        if backend not in {"registry", "s3"}:
            raise ValueError("snapshot publication backend is invalid")
        expected_schemes = ("s3://",) if backend == "s3" else ("http://", "https://")
        if not repo_blob_url.startswith(expected_schemes):
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
                PublishedStorageLayer.from_dict(layer) for layer in layers_raw
            ),
            backend=backend,
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
        max_concurrent_publications: int = DEFAULT_MAX_CONCURRENT_PUBLICATIONS,
        compact_after_layers: int = DEFAULT_COMPACT_AFTER_LAYERS,
        compact_after_bytes: int = DEFAULT_COMPACT_AFTER_BYTES,
        telemetry: Telemetry | None = None,
    ) -> None:
        if (
            not repository
            or repository.startswith("/")
            or ".." in repository.split("/")
        ):
            raise ValueError("snapshot repository is invalid")
        if not stream_socket_root.is_absolute():
            raise ValueError("stream socket root must be absolute")
        if upload_chunk_bytes <= 0:
            raise ValueError("upload chunk size must be positive")
        if stream_timeout_seconds <= 0:
            raise ValueError("stream timeout must be positive")
        if max_concurrent_publications <= 0:
            raise ValueError("publication concurrency must be positive")
        if compact_after_layers < 1:
            raise ValueError("compaction layer threshold must be positive")
        if compact_after_bytes < 1:
            raise ValueError("compaction byte threshold must be positive")
        self.registry = registry
        self.repository = repository
        self.stream_socket_root = stream_socket_root
        self.upload_chunk_bytes = upload_chunk_bytes
        self.stream_timeout_seconds = stream_timeout_seconds
        self.compact_after_layers = compact_after_layers
        self.compact_after_bytes = compact_after_bytes
        self.telemetry = telemetry or Telemetry.disabled("registry-snapshot-publisher")
        self._publication_gate = PublicationGate(max_concurrent_publications)
        self._completed_uploads = CompletedLayerUploads[PublishedStorageLayer]()
        self._metrics_lock = threading.Lock()
        self._publications = 0
        self._compactions = 0
        self._uploaded_bytes = 0
        self._publication_duration_ms = 0
        self._publication_duration_max_ms = 0
        self._compaction_input_layers = 0
        self._compaction_input_bytes = 0
        self._compaction_output_bytes = 0

    @property
    def repo_blob_url(self) -> str:
        repository = quote(self.repository, safe="/")
        return f"{self.registry.base_url}/v2/{repository}/blobs"

    def local_layer_sources(self, paths: tuple[Path, ...]) -> dict[str, Path]:
        return self._completed_uploads.completed_dense_sources(paths)

    def publish(
        self,
        *,
        exporter: DenseLayerExporter,
        source_layer_paths: tuple[Path, ...],
        virtual_size: int,
        existing_layers: tuple[PublishedStorageLayer, ...] = (),
        existing_repo_blob_url: str = "",
        global_config_path: Path | None = None,
        check_current: Callable[[], None] | None = None,
    ) -> StorageSnapshotPublication:
        started = time.monotonic()
        with self.telemetry.span(
            "snapshot.publish",
            attributes={
                "snapshot.backend": "registry",
                "snapshot.source_layer_count": len(source_layer_paths),
                "snapshot.existing_layer_count": len(existing_layers),
                "snapshot.virtual_size": virtual_size,
            },
        ) as span:
            with self._publication_gate.acquire(self.telemetry, check_current) as queue_wait_ms:
                span.set_attribute("snapshot.queue.wait_ms", queue_wait_ms)
                publication, compacted, uploaded_bytes = self._publish_locked(
                    exporter=exporter,
                    source_layer_paths=source_layer_paths,
                    virtual_size=virtual_size,
                    existing_layers=existing_layers,
                    existing_repo_blob_url=existing_repo_blob_url,
                    global_config_path=global_config_path,
                    check_current=check_current,
                )
            span.set_attributes(
                {
                    "snapshot.compacted": compacted,
                    "snapshot.uploaded_bytes": uploaded_bytes,
                    "snapshot.result_layer_count": len(publication.layers),
                }
            )
        elapsed_ms = int((time.monotonic() - started) * 1000)
        with self._metrics_lock:
            self._publications += 1
            self._compactions += int(compacted)
            self._uploaded_bytes += uploaded_bytes
            self._publication_duration_ms += elapsed_ms
            self._publication_duration_max_ms = max(
                self._publication_duration_max_ms, elapsed_ms
            )
        return publication

    def metrics(self) -> dict[str, int]:
        with self._metrics_lock:
            return {
                "snapshot_publications": self._publications,
                "snapshot_compactions": self._compactions,
                "snapshot_uploaded_bytes": self._uploaded_bytes,
                "snapshot_publication_duration_ms_total": self._publication_duration_ms,
                "snapshot_publication_duration_ms_max": self._publication_duration_max_ms,
                "snapshot_compaction_input_layers": self._compaction_input_layers,
                "snapshot_compaction_input_bytes": self._compaction_input_bytes,
                "snapshot_compaction_output_bytes": self._compaction_output_bytes,
                "snapshot_compact_after_layers": self.compact_after_layers,
                "snapshot_compact_after_bytes": self.compact_after_bytes,
                **self._publication_gate.metrics(),
                **self._completed_uploads.metrics(),
            }

    def verify(
        self,
        publication: StorageSnapshotPublication,
    ) -> StorageSnapshotPublication:
        with self.telemetry.span(
            "snapshot.verify",
            attributes={
                "snapshot.backend": "registry",
                "snapshot.layer_count": len(publication.layers),
            },
        ):
            return self._verify_unobserved(publication)

    def _verify_unobserved(
        self,
        publication: StorageSnapshotPublication,
    ) -> StorageSnapshotPublication:
        if publication.backend != "registry":
            raise ValueError("snapshot publication is not Registry-backed")
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
            or f"sha256:{hashlib.sha256(config_payload).hexdigest()}" != config_digest
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
            PublishedStorageLayer.from_dict(layer) for layer in raw_layers
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
        existing_repo_blob_url: str,
        global_config_path: Path | None,
        check_current: Callable[[], None] | None = None,
    ) -> tuple[StorageSnapshotPublication, bool, int]:
        if virtual_size <= 0:
            raise ValueError("snapshot virtual size must be positive")
        if not source_layer_paths and not existing_layers:
            raise ValueError("snapshot requires at least one sealed layer")
        for path in source_layer_paths:
            if not path.is_absolute():
                raise ValueError("sealed layer path must be absolute")
        input_layers = len(existing_layers) + len(source_layer_paths)
        layer_sizes = tuple(layer.size for layer in existing_layers) + tuple(
            local_layer_data_bytes(path) for path in source_layer_paths
        )
        compact_start = snapshot_compaction_start(
            layer_sizes, max_layers=self.compact_after_layers,
            max_delta_bytes=self.compact_after_bytes,
            reusable_base=bool(existing_layers),
            origin_changed=bool(
                existing_layers and existing_repo_blob_url
                and existing_repo_blob_url.rstrip("/") != self.repo_blob_url.rstrip("/")
            ),
        )
        should_compact = compact_start is not None
        retained_layers = existing_layers[:compact_start] if should_compact else ()
        if should_compact:
            if global_config_path is None or not global_config_path.is_absolute():
                raise ValueError(
                    "compacted publication requires an absolute global config path"
                )
            with self.telemetry.span(
                "snapshot.compact_and_upload",
                attributes={
                    "snapshot.input_layer_count": input_layers - len(retained_layers),
                    "snapshot.input_bytes": sum(layer_sizes[compact_start:]),
                    "snapshot.retained_base_bytes": sum(layer.size for layer in retained_layers),
                },
            ):
                layer, uploaded_bytes = self._completed_uploads.publish(
                    identity=lambda: (
                        "compact", existing_repo_blob_url,
                        tuple((item.digest, item.size) for item in existing_layers[compact_start:]),
                        local_layer_identity(global_config_path),
                        tuple(local_layer_identity(path) for path in source_layer_paths),
                    ),
                    exists=lambda item: self.registry.blob_exists(self.repository, item.digest),
                    check_current=check_current,
                    upload=lambda: self._publish_compacted_layer(
                        exporter,
                        existing_layers=existing_layers[compact_start:],
                        existing_repo_blob_url=existing_repo_blob_url,
                        source_layer_paths=source_layer_paths,
                        global_config_path=global_config_path,
                        check_current=check_current,
                    ),
                )
                layers = (*retained_layers, layer)
        else:
            with self.telemetry.span(
                "snapshot.export_and_upload",
                attributes={"snapshot.source_layer_count": len(source_layer_paths)},
            ):
                results = []
                for source in source_layer_paths:
                    results.append(self._completed_uploads.publish(
                        identity=lambda: ("dense", local_layer_identity(source)),
                        exists=lambda item: self.registry.blob_exists(self.repository, item.digest),
                        check_current=check_current,
                        upload=lambda: self._publish_dense_layer(exporter, source, check_current=check_current),
                    ))
                new_layers = tuple(layer for layer, _ in results)
            layers = (*existing_layers, *new_layers)
            uploaded_bytes = sum(size for _, size in results)
        with self.telemetry.span("snapshot.commit_metadata"):
            if check_current is not None:
                check_current()
            config = self._snapshot_config(virtual_size=virtual_size, layers=layers)
            config_digest = self._upload_bytes(config)
            tag = f"ucloud-storage-v1-{config_digest.removeprefix('sha256:')}"
            manifest = self._oci_manifest(
                config_digest=config_digest,
                config_size=len(config),
                layers=layers,
            )
            if check_current is not None:
                check_current()
            manifest_digest = self.registry.put_manifest(
                self.repository,
                tag,
                manifest,
                media_type=OCI_MANIFEST_MEDIA_TYPE,
            )
        publication = StorageSnapshotPublication(
            manifest_digest=manifest_digest,
            tag=tag,
            repository=self.repository,
            repo_blob_url=self.repo_blob_url,
            virtual_size=virtual_size,
            layers=layers,
            backend="registry",
        )
        with self._metrics_lock:
            if should_compact:
                self._compaction_input_layers += input_layers - len(retained_layers)
                self._compaction_input_bytes += sum(layer_sizes[compact_start:])
                self._compaction_output_bytes += layers[-1].size
        return publication, should_compact, uploaded_bytes

    def _publish_dense_layer(
        self,
        exporter: DenseLayerExporter,
        source_layer_path: Path,
        *, check_current: Callable[[], None] | None = None,
    ) -> PublishedStorageLayer:
        if not source_layer_path.is_absolute():
            raise ValueError("sealed layer path must be absolute")
        return self._publish_stream(
            lambda stream_socket_path: exporter.export_dense_layer(
                source_layer_path=source_layer_path,
                stream_socket_path=stream_socket_path,
            ),
            check_current=check_current,
        )

    def _publish_compacted_layer(
        self,
        exporter: DenseLayerExporter,
        *,
        existing_layers: tuple[PublishedStorageLayer, ...],
        existing_repo_blob_url: str,
        source_layer_paths: tuple[Path, ...],
        global_config_path: Path,
        check_current: Callable[[], None] | None = None,
    ) -> PublishedStorageLayer:
        self.stream_socket_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="compact-config-",
            dir=self.stream_socket_root,
        ) as raw_dir:
            source_config = Path(raw_dir) / "source.json"
            source_config.write_text(
                json.dumps(
                    {
                        "lowers": [
                            *(layer.to_dict() for layer in existing_layers),
                            *({"file": str(path)} for path in source_layer_paths),
                        ],
                        "repoBlobUrl": (
                            (existing_repo_blob_url or self.repo_blob_url)
                            if existing_layers
                            else ""
                        ),
                        "resultFile": "",
                        "upper": {},
                    },
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n",
                encoding="ascii",
            )
            return self._publish_stream(
                lambda stream_socket_path: exporter.export_compacted_image(
                    source_image_config=source_config,
                    global_config=global_config_path,
                    stream_socket_path=stream_socket_path,
                ),
                check_current=check_current,
            )

    def _publish_stream(
        self,
        export_layer: Callable[[Path], StorageNativeLayer],
        *, check_current: Callable[[], None] | None = None,
    ) -> PublishedStorageLayer:
        if check_current is not None:
            check_current()
        upload_location_holder = [self.registry.start_blob_upload(self.repository)]
        completed = False

        def consume(chunk: bytes) -> None:
            if check_current is not None:
                check_current()
            upload_location_holder[0] = self.registry.upload_blob_chunk(
                upload_location_holder[0], chunk
            )

        try:
            observed = consume_export_stream(
                export_layer,
                stream_socket_root=self.stream_socket_root,
                chunk_bytes=self.upload_chunk_bytes,
                timeout_seconds=self.stream_timeout_seconds,
                consume=consume,
                check_current=check_current,
            )
            if check_current is not None:
                check_current()
            self.registry.finish_blob_upload(upload_location_holder[0], observed.digest)
            completed = True
            if not self.registry.blob_exists(self.repository, observed.digest):
                raise ValueError("registry did not retain the uploaded dense layer")
            return PublishedStorageLayer(digest=observed.digest, size=observed.size)
        finally:
            if not completed:
                try:
                    self.registry.abort_blob_upload(upload_location_holder[0])
                except Exception:
                    # Preserve the original cancellation/transport error. The
                    # registry's normal upload purge handles failed cleanup.
                    pass

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
                            "containerd.io/snapshot/overlaybd/blob-digest": layer.digest,
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


class SnapshotPublisherRouter:
    """Publish to one backend while retaining old-backend wake compatibility."""

    def __init__(
        self,
        primary: SnapshotPublisher,
        *,
        verifiers: dict[str, SnapshotPublisher],
    ) -> None:
        self.primary = primary
        self.verifiers = dict(verifiers)

    def publish(
        self,
        *,
        exporter: DenseLayerExporter,
        source_layer_paths: tuple[Path, ...],
        virtual_size: int,
        existing_layers: tuple[PublishedStorageLayer, ...] = (),
        existing_repo_blob_url: str = "",
        global_config_path: Path | None = None,
        check_current: Callable[[], None] | None = None,
    ) -> StorageSnapshotPublication:
        return self.primary.publish(
            exporter=exporter,
            source_layer_paths=source_layer_paths,
            virtual_size=virtual_size,
            existing_layers=existing_layers,
            existing_repo_blob_url=existing_repo_blob_url,
            global_config_path=global_config_path,
            check_current=check_current,
        )

    def verify(
        self, publication: StorageSnapshotPublication
    ) -> StorageSnapshotPublication:
        verifier = self.verifiers.get(publication.backend)
        if verifier is None:
            raise ValueError(
                f"snapshot backend {publication.backend!r} is not configured"
            )
        return verifier.verify(publication)

    def metrics(self) -> dict[str, int]:
        return self.primary.metrics()

    def local_layer_sources(self, paths: tuple[Path, ...]) -> dict[str, Path]:
        lookup = getattr(self.primary, "local_layer_sources", None)
        return lookup(paths) if lookup is not None else {}


def consume_export_stream(
    export_layer: Callable[[Path], StorageNativeLayer],
    *,
    stream_socket_root: Path,
    chunk_bytes: int,
    timeout_seconds: float,
    consume: Callable[[bytes], None],
    check_current: Callable[[], None] | None = None,
) -> StorageNativeLayer:
    """Validate an AgentEnv export while forwarding bounded chunks."""

    if check_current is not None:
        check_current()
    stream_socket_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="dense-", dir=stream_socket_root
    ) as raw_dir:
        socket_path = Path(raw_dir) / "stream.sock"
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(socket_path))
            socket_path.chmod(0o600)
            listener.listen(1)
            listener.settimeout(min(0.1, timeout_seconds))
            result: list[StorageNativeLayer] = []
            failure: list[BaseException] = []

            def export() -> None:
                try:
                    result.append(export_layer(socket_path))
                except BaseException as exc:
                    failure.append(exc)

            thread = threading.Thread(target=export, daemon=True)
            thread.start()
            hasher = hashlib.sha256()
            byte_count = 0
            # Receive directly into one reusable upload buffer. Appending recv
            # results and slicing a bytearray copied each layer several times.
            pending = bytearray(chunk_bytes)
            pending_view = memoryview(pending)
            pending_size = 0
            try:
                accept_deadline = time.monotonic() + timeout_seconds
                while True:
                    if check_current is not None:
                        check_current()
                    try:
                        connection, _ = listener.accept()
                        break
                    except socket.timeout:
                        if not thread.is_alive():
                            thread.join()
                            if failure:
                                raise failure[0]
                            raise RuntimeError(
                                "dense layer exporter exited before connecting"
                            )
                        if time.monotonic() >= accept_deadline:
                            raise TimeoutError("dense layer exporter did not connect")
                with connection:
                    connection.settimeout(min(0.25, timeout_seconds))
                    read_deadline = time.monotonic() + timeout_seconds
                    next_check = 0.0
                    while True:
                        now = time.monotonic()
                        if check_current is not None and now >= next_check:
                            check_current()
                            next_check = now + 0.25
                        try:
                            received = connection.recv_into(pending_view[pending_size:])
                        except socket.timeout:
                            if time.monotonic() >= read_deadline:
                                raise TimeoutError("dense layer exporter stopped streaming")
                            continue
                        read_deadline = time.monotonic() + timeout_seconds
                        if not received:
                            break
                        byte_count += received
                        pending_size += received
                        if pending_size == chunk_bytes:
                            hasher.update(pending_view)
                            # Consumers may retain chunks for asynchronous S3
                            # uploads, so hand off immutable, independent bytes.
                            consume(bytes(pending))
                            pending_size = 0
                            read_deadline = time.monotonic() + timeout_seconds
                    if pending_size:
                        tail = pending_view[:pending_size]
                        hasher.update(tail)
                        consume(bytes(tail))
            finally:
                # Refuse a late exporter connection after cancellation.
                listener.close()
                thread.join(timeout=timeout_seconds)
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
        raise ValueError("dense layer stream does not match the backend descriptor")
    return observed


def _is_digest(value: str) -> bool:
    return (
        value.startswith("sha256:")
        and len(value) == 71
        and all(character in "0123456789abcdef" for character in value[7:])
    )
