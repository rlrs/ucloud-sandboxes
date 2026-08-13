from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import tempfile
import threading
import time
from typing import Any, Callable, Protocol
from uuid import uuid4

from .config import normalize_s3_endpoint
from .storage_native import StorageNativeLayer
from .storage_native_registry import (
    DEFAULT_COMPACT_AFTER_BYTES,
    DEFAULT_COMPACT_AFTER_LAYERS,
    DenseLayerExporter,
    PublishedStorageLayer,
    RegistrySnapshotPublisher,
    StorageSnapshotPublication,
    consume_export_stream,
)


MINIMUM_MULTIPART_CHUNK_BYTES = 5 * 1024 * 1024
HETZNER_TRANSIENT_RETRY_ATTEMPTS = 8


@dataclass(frozen=True)
class S3ObjectStat:
    size: int
    sha256: str = ""
    modified_at: float = 0.0


class S3ObjectClient(Protocol):
    def create_multipart_upload(self, key: str) -> str: ...

    def upload_part(
        self, key: str, upload_id: str, part_number: int, payload: bytes
    ) -> str: ...

    def complete_multipart_upload(
        self,
        key: str,
        upload_id: str,
        parts: tuple[tuple[int, str], ...],
    ) -> None: ...

    def abort_multipart_upload(self, key: str, upload_id: str) -> None: ...

    def stat(self, key: str) -> S3ObjectStat | None: ...

    def put_bytes(self, key: str, payload: bytes, *, sha256: str) -> None: ...

    def get_bytes(self, key: str, *, max_bytes: int) -> bytes: ...

    def copy(self, source_key: str, destination_key: str, *, size: int) -> None: ...

    def delete(self, key: str) -> None: ...

    def list_objects(self, prefix: str) -> tuple[tuple[str, S3ObjectStat], ...]: ...


class Boto3S3ObjectClient:
    """Small boto3 adapter with server-side multipart copy and bounded retries."""

    def __init__(
        self,
        *,
        endpoint: str,
        bucket: str,
        region: str,
        credential_process: str = "",
        credentials: dict[str, str] | None = None,
    ) -> None:
        endpoint = normalize_s3_endpoint(endpoint, bucket=bucket, region=region)
        resolved_credentials = credentials or _resolve_credential_process(
            credential_process
        )
        try:
            import boto3
            from boto3.s3.transfer import TransferConfig
            from botocore.config import Config
            from botocore.exceptions import ClientError
        except ImportError as exc:  # pragma: no cover - packaging failure on a node
            raise RuntimeError("S3 snapshot publication requires boto3") from exc
        self.bucket = bucket
        self._client_error = ClientError
        self._transfer_config = TransferConfig(
            multipart_threshold=64 * 1024 * 1024,
            multipart_chunksize=64 * 1024 * 1024,
            max_concurrency=4,
            use_threads=True,
        )
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=resolved_credentials["access_key_id"],
            aws_secret_access_key=resolved_credentials["secret_access_key"],
            aws_session_token=resolved_credentials.get("security_token") or None,
            config=Config(
                retries={"max_attempts": 8, "mode": "adaptive"},
                connect_timeout=10,
                read_timeout=120,
                max_pool_connections=32,
                signature_version="s3v4",
                # Hetzner's documented boto3 configuration uses virtual-host
                # addressing and unsigned payloads. SigV4 still authenticates
                # every request; optional modern SDK checksums are only sent
                # when the operation requires them.
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
                s3={
                    "addressing_style": "virtual",
                    "payload_signing_enabled": False,
                },
            ),
        )
        if endpoint.endswith(".your-objectstorage.com"):
            # New Hetzner buckets/keys can briefly be inconsistent across S3
            # gateway instances. Botocore does not normally retry these 4xx
            # responses, so make a bounded Hetzner-only exception.
            self._client.meta.events.register_first(
                "needs-retry.s3", _retry_transient_hetzner_gateway
            )
            # Some Hetzner gateways reject boto3's 100-continue request path.
            # Expect is not a signed header, so removal preserves SigV4.
            self._client.meta.events.register_first(
                "before-send.s3.*", _strip_expect_header
            )

    def create_multipart_upload(self, key: str) -> str:
        response = self._client.create_multipart_upload(
            Bucket=self.bucket,
            Key=key,
            Metadata={"ucloud-content": "streaming-snapshot-layer"},
        )
        return str(response["UploadId"])

    def upload_part(
        self, key: str, upload_id: str, part_number: int, payload: bytes
    ) -> str:
        response = self._client.upload_part(
            Bucket=self.bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=payload,
        )
        return str(response["ETag"])

    def complete_multipart_upload(
        self,
        key: str,
        upload_id: str,
        parts: tuple[tuple[int, str], ...],
    ) -> None:
        self._client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": part_number, "ETag": etag}
                    for part_number, etag in parts
                ]
            },
        )

    def abort_multipart_upload(self, key: str, upload_id: str) -> None:
        self._client.abort_multipart_upload(
            Bucket=self.bucket,
            Key=key,
            UploadId=upload_id,
        )

    def stat(self, key: str) -> S3ObjectStat | None:
        try:
            response = self._client.head_object(Bucket=self.bucket, Key=key)
        except self._client_error as exc:
            status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        metadata = response.get("Metadata") or {}
        return S3ObjectStat(
            size=int(response["ContentLength"]),
            sha256=str(metadata.get("sha256") or ""),
            modified_at=(
                response["LastModified"].timestamp()
                if response.get("LastModified") is not None
                else 0.0
            ),
        )

    def put_bytes(self, key: str, payload: bytes, *, sha256: str) -> None:
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=payload,
            ContentType="application/json",
            Metadata={"sha256": sha256},
        )

    def get_bytes(self, key: str, *, max_bytes: int) -> bytes:
        response = self._client.get_object(Bucket=self.bucket, Key=key)
        length = int(response.get("ContentLength") or 0)
        if length > max_bytes:
            response["Body"].close()
            raise ValueError("S3 snapshot metadata exceeds its size limit")
        payload = response["Body"].read(max_bytes + 1)
        response["Body"].close()
        if len(payload) > max_bytes:
            raise ValueError("S3 snapshot metadata exceeds its size limit")
        return payload

    def copy(self, source_key: str, destination_key: str, *, size: int) -> None:
        self._client.copy(
            {"Bucket": self.bucket, "Key": source_key},
            self.bucket,
            destination_key,
            Config=self._transfer_config,
            ExtraArgs={"Metadata": {"sha256": destination_key.rsplit("/", 1)[-1]}, "MetadataDirective": "REPLACE"},
        )

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self.bucket, Key=key)

    def list_objects(self, prefix: str) -> tuple[tuple[str, S3ObjectStat], ...]:
        result: list[tuple[str, S3ObjectStat]] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for raw in page.get("Contents") or ():
                modified = raw.get("LastModified")
                result.append(
                    (
                        str(raw["Key"]),
                        S3ObjectStat(
                            size=int(raw["Size"]),
                            modified_at=(
                                modified.timestamp() if modified is not None else 0.0
                            ),
                        ),
                    )
                )
        return tuple(result)


class S3SnapshotPublisher:
    """Publish portable layers directly from a worker to S3-compatible storage."""

    def __init__(
        self,
        *,
        endpoint: str,
        bucket: str,
        region: str,
        prefix: str,
        credential_process: str,
        stream_socket_root: Path,
        upload_chunk_bytes: int = 64 * 1024 * 1024,
        upload_part_concurrency: int = 4,
        stream_timeout_seconds: float = 120.0,
        max_concurrent_publications: int = 2,
        compact_after_layers: int = DEFAULT_COMPACT_AFTER_LAYERS,
        compact_after_bytes: int = DEFAULT_COMPACT_AFTER_BYTES,
        client_factory: Callable[[], S3ObjectClient] | None = None,
    ) -> None:
        normalized_prefix = prefix.strip("/")
        if not endpoint.startswith(("http://", "https://")):
            raise ValueError("S3 endpoint must be HTTP(S)")
        if not bucket or "/" in bucket:
            raise ValueError("S3 bucket is invalid")
        if not region:
            raise ValueError("S3 region is required")
        if not normalized_prefix or ".." in normalized_prefix.split("/"):
            raise ValueError("S3 snapshot prefix is invalid")
        if not credential_process:
            raise ValueError("S3 credential process is required")
        if not stream_socket_root.is_absolute():
            raise ValueError("stream socket root must be absolute")
        if upload_chunk_bytes < MINIMUM_MULTIPART_CHUNK_BYTES:
            raise ValueError("S3 multipart chunks must be at least 5 MiB")
        if upload_part_concurrency < 1:
            raise ValueError("S3 multipart upload concurrency must be positive")
        if stream_timeout_seconds <= 0:
            raise ValueError("stream timeout must be positive")
        if max_concurrent_publications <= 0:
            raise ValueError("publication concurrency must be positive")
        if compact_after_layers < 1 or compact_after_bytes < 1:
            raise ValueError("compaction thresholds must be positive")
        self.endpoint = normalize_s3_endpoint(
            endpoint, bucket=bucket, region=region
        )
        self.bucket = bucket
        self.region = region
        self.prefix = normalized_prefix
        self.credential_process = credential_process
        self.stream_socket_root = stream_socket_root
        self.upload_chunk_bytes = upload_chunk_bytes
        self.upload_part_concurrency = upload_part_concurrency
        self.stream_timeout_seconds = stream_timeout_seconds
        self.compact_after_layers = compact_after_layers
        self.compact_after_bytes = compact_after_bytes
        self.repository = f"{bucket}/{normalized_prefix}"
        self._client_factory = client_factory or (
            lambda: Boto3S3ObjectClient(
                endpoint=self.endpoint,
                bucket=self.bucket,
                region=self.region,
                credential_process=self.credential_process,
            )
        )
        self._publication_slots = threading.BoundedSemaphore(max_concurrent_publications)
        self._metrics_lock = threading.Lock()
        self._publications = 0
        self._compactions = 0
        self._uploaded_bytes = 0
        self._publication_duration_ms = 0
        self._publication_duration_max_ms = 0

    @property
    def repo_blob_url(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}/managed-layers"

    def publish(
        self,
        *,
        exporter: DenseLayerExporter,
        source_layer_paths: tuple[Path, ...],
        virtual_size: int,
        existing_layers: tuple[PublishedStorageLayer, ...] = (),
        existing_repo_blob_url: str = "",
        global_config_path: Path | None = None,
    ) -> StorageSnapshotPublication:
        started = time.monotonic()
        with self._publication_slots:
            publication, compacted, uploaded_bytes = self._publish_locked(
                client=self._client_factory(),
                exporter=exporter,
                source_layer_paths=source_layer_paths,
                virtual_size=virtual_size,
                existing_layers=existing_layers,
                existing_repo_blob_url=existing_repo_blob_url,
                global_config_path=global_config_path,
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

    def verify(
        self, publication: StorageSnapshotPublication
    ) -> StorageSnapshotPublication:
        if publication.backend != "s3":
            raise ValueError("snapshot publication is not S3-backed")
        if publication.repository != self.repository:
            raise ValueError("snapshot publication belongs to another S3 repository")
        if publication.repo_blob_url != self.repo_blob_url:
            raise ValueError("snapshot S3 blob URL is not configured")
        client = self._client_factory()
        manifest_key = self._manifest_key(publication.manifest_digest)
        manifest_payload = client.get_bytes(manifest_key, max_bytes=1024 * 1024)
        if _digest(manifest_payload) != publication.manifest_digest:
            raise ValueError("S3 snapshot manifest content is corrupt")
        try:
            manifest = json.loads(manifest_payload.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("S3 snapshot manifest is invalid JSON") from exc
        config_raw = manifest.get("config") if isinstance(manifest, dict) else None
        layers_raw = manifest.get("layers") if isinstance(manifest, dict) else None
        if not isinstance(config_raw, dict) or not isinstance(layers_raw, list):
            raise ValueError("S3 snapshot manifest is malformed")
        config_digest = str(config_raw.get("digest") or "")
        config_size = config_raw.get("size")
        if not _valid_digest(config_digest) or not isinstance(config_size, int):
            raise ValueError("S3 snapshot config descriptor is invalid")
        config_payload = client.get_bytes(
            self._metadata_key(config_digest), max_bytes=1024 * 1024
        )
        if len(config_payload) != config_size or _digest(config_payload) != config_digest:
            raise ValueError("S3 snapshot config content is corrupt")
        expected = RegistrySnapshotPublisher._snapshot_config(
            virtual_size=publication.virtual_size,
            layers=publication.layers,
        )
        if config_payload != expected:
            raise ValueError("S3 snapshot config does not match its publication")
        manifest_layers = tuple(PublishedStorageLayer.from_dict(raw) for raw in layers_raw)
        if manifest_layers != publication.layers:
            raise ValueError("S3 snapshot layers do not match its publication")
        for layer in publication.layers:
            stat = client.stat(self._layer_key(layer.digest))
            if stat is None or stat.size != layer.size:
                raise ValueError("S3 snapshot layer is missing or has the wrong size")
        return publication

    def metrics(self) -> dict[str, int]:
        with self._metrics_lock:
            return {
                "snapshot_publications": self._publications,
                "snapshot_compactions": self._compactions,
                "snapshot_object_upload_bytes": self._uploaded_bytes,
                "snapshot_publication_duration_ms_total": self._publication_duration_ms,
                "snapshot_publication_duration_ms_max": self._publication_duration_max_ms,
                "snapshot_compact_after_layers": self.compact_after_layers,
                "snapshot_compact_after_bytes": self.compact_after_bytes,
                "snapshot_upload_part_concurrency": self.upload_part_concurrency,
                "snapshot_upload_part_bytes": self.upload_chunk_bytes,
            }

    def _publish_locked(
        self,
        *,
        client: S3ObjectClient,
        exporter: DenseLayerExporter,
        source_layer_paths: tuple[Path, ...],
        virtual_size: int,
        existing_layers: tuple[PublishedStorageLayer, ...],
        existing_repo_blob_url: str,
        global_config_path: Path | None,
    ) -> tuple[StorageSnapshotPublication, bool, int]:
        if virtual_size <= 0:
            raise ValueError("snapshot virtual size must be positive")
        if not source_layer_paths and not existing_layers:
            raise ValueError("snapshot requires at least one sealed layer")
        input_bytes = sum(layer.size for layer in existing_layers)
        for path in source_layer_paths:
            if not path.is_absolute():
                raise ValueError("sealed layer path must be absolute")
            input_bytes += path.stat().st_size
        should_compact = (
            len(existing_layers) + len(source_layer_paths) > self.compact_after_layers
            or input_bytes > self.compact_after_bytes
            or bool(
                existing_layers
                and existing_repo_blob_url
                and existing_repo_blob_url.rstrip("/") != self.repo_blob_url.rstrip("/")
            )
        )
        uploaded_bytes = 0
        if should_compact:
            if global_config_path is None or not global_config_path.is_absolute():
                raise ValueError("compacted publication requires a global config")
            layer = self._publish_compacted_layer(
                client,
                exporter,
                existing_layers=existing_layers,
                existing_repo_blob_url=existing_repo_blob_url,
                source_layer_paths=source_layer_paths,
                global_config_path=global_config_path,
            )
            layers = (layer,)
            uploaded_bytes += layer.size
        else:
            new_layers = tuple(
                self._publish_dense_layer(client, exporter, path)
                for path in source_layer_paths
            )
            layers = (*existing_layers, *new_layers)
            uploaded_bytes += sum(layer.size for layer in new_layers)

        config = RegistrySnapshotPublisher._snapshot_config(
            virtual_size=virtual_size, layers=layers
        )
        config_digest = _digest(config)
        self._put_content_addressed(
            client, self._metadata_key(config_digest), config, config_digest
        )
        tag = f"ucloud-storage-v1-{config_digest.removeprefix('sha256:')}"
        manifest = RegistrySnapshotPublisher._oci_manifest(
            config_digest=config_digest,
            config_size=len(config),
            layers=layers,
        )
        manifest_digest = _digest(manifest)
        self._put_content_addressed(
            client,
            self._manifest_key(manifest_digest),
            manifest,
            manifest_digest,
        )
        return (
            StorageSnapshotPublication(
                manifest_digest=manifest_digest,
                tag=tag,
                repository=self.repository,
                repo_blob_url=self.repo_blob_url,
                virtual_size=virtual_size,
                layers=layers,
                backend="s3",
            ),
            should_compact,
            uploaded_bytes,
        )

    def _publish_dense_layer(
        self,
        client: S3ObjectClient,
        exporter: DenseLayerExporter,
        source: Path,
    ) -> PublishedStorageLayer:
        return self._publish_stream(
            client,
            lambda socket_path: exporter.export_dense_layer(
                source_layer_path=source,
                stream_socket_path=socket_path,
            ),
        )

    def _publish_compacted_layer(
        self,
        client: S3ObjectClient,
        exporter: DenseLayerExporter,
        *,
        existing_layers: tuple[PublishedStorageLayer, ...],
        existing_repo_blob_url: str,
        source_layer_paths: tuple[Path, ...],
        global_config_path: Path,
    ) -> PublishedStorageLayer:
        self.stream_socket_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="compact-config-", dir=self.stream_socket_root
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
                client,
                lambda socket_path: exporter.export_compacted_image(
                    source_image_config=source_config,
                    global_config=global_config_path,
                    stream_socket_path=socket_path,
                ),
            )

    def _publish_stream(
        self,
        client: S3ObjectClient,
        export_layer: Callable[[Path], StorageNativeLayer],
    ) -> PublishedStorageLayer:
        temporary_key = f"{self.prefix}/.uploads/{uuid4().hex}"
        upload_id = client.create_multipart_upload(temporary_key)
        parts: list[tuple[int, str]] = []
        pending: list[tuple[int, Future[str]]] = []
        completed = False

        def finish_oldest() -> None:
            part_number, future = pending.pop(0)
            etag = future.result()
            parts.append((part_number, etag))

        try:
            with ThreadPoolExecutor(
                max_workers=self.upload_part_concurrency,
                thread_name_prefix="snapshot-s3-part",
            ) as executor:
                next_part_number = 1

                def consume(payload: bytes) -> None:
                    nonlocal next_part_number
                    if len(pending) >= self.upload_part_concurrency:
                        finish_oldest()
                    part_number = next_part_number
                    next_part_number += 1
                    pending.append(
                        (
                            part_number,
                            executor.submit(
                                client.upload_part,
                                temporary_key,
                                upload_id,
                                part_number,
                                payload,
                            ),
                        )
                    )

                observed = consume_export_stream(
                    export_layer,
                    stream_socket_root=self.stream_socket_root,
                    chunk_bytes=self.upload_chunk_bytes,
                    timeout_seconds=self.stream_timeout_seconds,
                    consume=consume,
                )
                while pending:
                    finish_oldest()
            try:
                client.complete_multipart_upload(
                    temporary_key,
                    upload_id,
                    tuple(sorted(parts)),
                )
            except BaseException:
                # CompleteMultipartUpload is not safely repeatable after the
                # server commits but the response is lost: a retry can return
                # NoSuchUpload even though the object is already durable.
                # Resolve that ambiguity by checking the exact object size.
                # A wrong/missing object still takes the normal failure path.
                ambiguous = client.stat(temporary_key)
                if ambiguous is None or ambiguous.size != observed.size:
                    raise
            completed = True
            temporary = client.stat(temporary_key)
            if temporary is None or temporary.size != observed.size:
                raise ValueError("S3 did not retain the complete streamed layer")
            destination_key = self._layer_key(observed.digest)
            destination = client.stat(destination_key)
            if destination is None:
                client.copy(temporary_key, destination_key, size=observed.size)
                destination = client.stat(destination_key)
            if destination is None or destination.size != observed.size:
                raise ValueError("S3 did not retain the content-addressed layer")
            return PublishedStorageLayer(observed.digest, observed.size)
        finally:
            if not completed:
                try:
                    client.abort_multipart_upload(temporary_key, upload_id)
                except BaseException:
                    pass
            else:
                try:
                    client.delete(temporary_key)
                except BaseException:
                    pass

    def _put_content_addressed(
        self,
        client: S3ObjectClient,
        key: str,
        payload: bytes,
        digest: str,
    ) -> None:
        existing = client.stat(key)
        if existing is None:
            client.put_bytes(key, payload, sha256=digest)
            existing = client.stat(key)
        if existing is None or existing.size != len(payload):
            raise ValueError("S3 did not retain content-addressed snapshot metadata")
        observed = client.get_bytes(key, max_bytes=1024 * 1024)
        if observed != payload:
            raise ValueError("S3 content-addressed snapshot metadata is corrupt")

    def _layer_key(self, digest: str) -> str:
        return f"{self.prefix}/managed-layers/{digest}"

    def _metadata_key(self, digest: str) -> str:
        return f"{self.prefix}/metadata/{digest}.json"

    def _manifest_key(self, digest: str) -> str:
        return f"{self.prefix}/manifests/{digest}.json"


def _resolve_credential_process(command: str) -> dict[str, str]:
    argv = shlex.split(command)
    if not argv:
        raise ValueError("S3 credential process is empty")
    result = subprocess.run(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "S3 credential process failed: " + result.stderr.strip()[:1024]
        )
    try:
        raw = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ValueError("S3 credential process returned invalid JSON") from exc
    if not isinstance(raw, dict):
        raise ValueError("S3 credential process must return an object")
    access_key_id = str(
        raw.get("AccessKeyId") or raw.get("accessKeyId") or raw.get("access_key_id") or ""
    ).strip()
    secret_access_key = str(
        raw.get("SecretAccessKey")
        or raw.get("AccessKeySecret")
        or raw.get("secretAccessKey")
        or raw.get("secret_access_key")
        or ""
    ).strip()
    security_token = str(
        raw.get("SecurityToken")
        or raw.get("SessionToken")
        or raw.get("securityToken")
        or raw.get("security_token")
        or ""
    ).strip()
    if not access_key_id or not secret_access_key:
        raise ValueError("S3 credential process omitted access or secret key")
    return {
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
        "security_token": security_token,
    }


def _strip_expect_header(request: Any, **_kwargs: Any) -> None:
    request.headers.pop("Expect", None)


def _retry_transient_hetzner_gateway(
    response: Any = None,
    attempts: int = 0,
    **_kwargs: Any,
) -> float | None:
    if response is None or not isinstance(response, tuple) or len(response) != 2:
        return None
    http_response, parsed = response
    status = int(getattr(http_response, "status_code", 0) or 0)
    error = parsed.get("Error") if isinstance(parsed, dict) else None
    code = str(error.get("Code") or "") if isinstance(error, dict) else ""
    if attempts >= HETZNER_TRANSIENT_RETRY_ATTEMPTS:
        return None
    if not (
        (status == 403 and code in {"", "403", "AccessDenied"})
        or (status == 404 and code == "NoSuchBucket")
    ):
        return None
    return min(0.1 * (2 ** max(attempts - 1, 0)), 2.0)


def _digest(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _valid_digest(value: str) -> bool:
    return (
        value.startswith("sha256:")
        and len(value) == 71
        and all(character in "0123456789abcdef" for character in value[7:])
    )
