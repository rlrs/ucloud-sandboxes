"""Sparse memory artifacts and a final OCI root for split checkpoints.

Workspace snapshots keep their existing block format. The root references both
immutable OCI manifests and is published only after all memory blobs commit.
Local lifecycle fencing stays with the caller; this module owns bytes only.
"""

from dataclasses import dataclass
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import struct
from typing import Callable

from .managed_registry import RegistryClient

OCI_IMAGE = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
MEMORY_CONFIG = "application/vnd.ucloud.checkpoint.memory.v1+json"
MEMORY_LAYER = "application/vnd.ucloud.checkpoint.sparse.v1"
MEMORY_SCHEMA = "ucloud-checkpoint-memory-v1"
ROOT_SCHEMA = "ucloud-checkpoint-v3"
CHUNK_BYTES = 1024 * 1024
MAX_HEADER = 1024 * 1024
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def digest_bytes(value):
    return "sha256:" + hashlib.sha256(value).hexdigest()


@dataclass(frozen=True)
class CheckpointReference:
    repository: str
    tag: str
    manifest_digest: str
    media_type: str
    size: int

    def __post_init__(self):
        if (
            not isinstance(self.repository, str)
            or not self.repository
            or ".." in self.repository.split("/")
            or not isinstance(self.tag, str)
            or not _NAME.fullmatch(self.tag)
            or not isinstance(self.manifest_digest, str)
            or not _DIGEST.fullmatch(self.manifest_digest)
            or not isinstance(self.media_type, str)
            or self.media_type not in {OCI_IMAGE, OCI_INDEX}
            or type(self.size) is not int
            or self.size <= 0
        ):
            raise ValueError("invalid checkpoint reference")

    @property
    def backend(self):
        return "registry"

    def to_dict(self):
        return {
            name: getattr(self, name)
            for name in ("repository", "tag", "manifest_digest", "media_type", "size")
        }

    def descriptor(self, component):
        return {
            "mediaType": self.media_type,
            "digest": self.manifest_digest,
            "size": self.size,
            "annotations": {"org.ucloud.component": component},
        }

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) != {
            "repository",
            "tag",
            "manifest_digest",
            "media_type",
            "size",
        }:
            raise ValueError("invalid checkpoint reference schema")
        return cls(**raw)


@dataclass(frozen=True)
class MemoryArtifactBlob:
    name: str
    digest: str
    size: int
    logical_bytes: int

    def __post_init__(self):
        if (
            not isinstance(self.name, str)
            or not _NAME.fullmatch(self.name)
            or self.name in {".", ".."}
            or not isinstance(self.digest, str)
            or not _DIGEST.fullmatch(self.digest)
            or type(self.size) is not int
            or self.size <= 0
            or type(self.logical_bytes) is not int
            or self.logical_bytes < 0
        ):
            raise ValueError("invalid memory artifact blob")

    def to_dict(self):
        return {
            name: getattr(self, name)
            for name in ("name", "digest", "size", "logical_bytes")
        }

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) != {
            "name",
            "digest",
            "size",
            "logical_bytes",
        }:
            raise ValueError("invalid memory artifact schema")
        return cls(**raw)


@dataclass(frozen=True)
class MemoryArtifactPublication:
    reference: CheckpointReference
    source_manifest_sha256: str
    files: tuple[MemoryArtifactBlob, ...]

    def __post_init__(self):
        if (
            not isinstance(self.source_manifest_sha256, str)
            or not re.fullmatch(r"[0-9a-f]{64}", self.source_manifest_sha256)
            or not isinstance(self.reference, CheckpointReference)
            or not isinstance(self.files, tuple)
            or not self.files
            or any(not isinstance(file, MemoryArtifactBlob) for file in self.files)
            or len({file.name for file in self.files}) != len(self.files)
            or self.reference.media_type != OCI_IMAGE
        ):
            raise ValueError("invalid memory publication")

    def to_dict(self):
        return {
            "reference": self.reference.to_dict(),
            "source_manifest_sha256": self.source_manifest_sha256,
            "files": [file.to_dict() for file in self.files],
        }

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) != {
            "reference",
            "source_manifest_sha256",
            "files",
        }:
            raise ValueError("invalid memory publication schema")
        if not isinstance(raw["files"], list):
            raise ValueError("invalid memory publication file inventory")
        return cls(
            CheckpointReference.from_dict(raw["reference"]),
            raw["source_manifest_sha256"],
            tuple(MemoryArtifactBlob.from_dict(file) for file in raw["files"]),
        )


def sparse_extents(fd: int, size: int):
    """Describe data without materializing holes; unsupported hosts use dense data."""
    extents, offset = [], 0
    while offset < size:
        try:
            start = os.lseek(fd, offset, os.SEEK_DATA)
            end = min(size, os.lseek(fd, start, os.SEEK_HOLE))
        except OSError as exc:
            if exc.errno == errno.ENXIO:
                break
            if exc.errno in {errno.EINVAL, errno.ENOTSUP}:
                return [(0, size)] if size else []
            raise
        if not offset <= start < end <= size:
            raise ValueError("filesystem returned invalid sparse extents")
        extents.append((start, end - start))
        offset = end
    return extents


def encoded_sparse_chunks(fd, name, *, check_current: Callable[[], None]):
    before = os.fstat(fd)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError("checkpoint source must be a regular file")
    extents = sparse_extents(fd, before.st_size)
    header = canonical_bytes(
        {"name": name, "logical_bytes": before.st_size, "extents": extents}
    )
    if len(header) > MAX_HEADER:
        raise ValueError("checkpoint sparse extent map is too large")
    check_current()
    yield struct.pack(">I", len(header)) + header
    for start, length in extents:
        offset = start
        while offset < start + length:
            check_current()
            chunk = os.pread(fd, min(CHUNK_BYTES, start + length - offset), offset)
            if not chunk:
                raise ValueError("checkpoint source changed during sparse upload")
            yield chunk
            offset += len(chunk)
    check_current()
    after = os.fstat(fd)
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        raise ValueError("checkpoint source changed during sparse upload")


class RegistryCheckpointStore:
    def __init__(self, registry: RegistryClient, *, repository: str):
        if (
            not isinstance(repository, str)
            or not repository
            or ".." in repository.split("/")
        ):
            raise ValueError("invalid checkpoint repository")
        self.registry, self.repository = registry, repository

    def _upload(self, chunks, check_current):
        location = self.registry.start_blob_upload(self.repository)
        digest, size = hashlib.sha256(), 0
        try:
            for chunk in chunks:
                check_current()
                location = self.registry.upload_blob_chunk(location, chunk)
                digest.update(chunk)
                size += len(chunk)
            check_current()
            result = "sha256:" + digest.hexdigest()
            self.registry.finish_blob_upload(location, result)
            return result, size
        except BaseException:
            try:
                self.registry.abort_blob_upload(location)
            except Exception:
                pass  # An ambiguous uploaded blob is never a portable root.
            raise

    def publish_memory(
        self, sources: dict[str, int], *, source_manifest_sha256: str, check_current
    ):
        files = []
        for name, fd in sorted(sources.items()):
            if not _NAME.fullmatch(name) or name in {".", ".."}:
                raise ValueError("invalid checkpoint artifact name")
            logical = os.fstat(fd).st_size
            digest, size = self._upload(
                encoded_sparse_chunks(fd, name, check_current=check_current),
                check_current,
            )
            files.append(MemoryArtifactBlob(name, digest, size, logical))
        config = {
            "schema": MEMORY_SCHEMA,
            "source_manifest_sha256": source_manifest_sha256,
            "files": [file.to_dict() for file in files],
        }
        payload = canonical_bytes(config)
        config_digest, config_size = self._upload((payload,), check_current)
        manifest = {
            "schemaVersion": 2,
            "mediaType": OCI_IMAGE,
            "config": {
                "mediaType": MEMORY_CONFIG,
                "digest": config_digest,
                "size": config_size,
            },
            "layers": [
                {"mediaType": MEMORY_LAYER, "digest": file.digest, "size": file.size}
                for file in files
            ],
        }
        reference = self._put_manifest(
            manifest, prefix="memory", check_current=check_current
        )
        publication = MemoryArtifactPublication(
            reference, source_manifest_sha256, tuple(files)
        )
        self.verify_memory(publication)
        return publication

    def _put_manifest(self, manifest, *, prefix, check_current):
        payload = canonical_bytes(manifest)
        digest = digest_bytes(payload)
        reference = CheckpointReference(
            self.repository,
            f"{prefix}-{digest[7:]}",
            digest,
            manifest["mediaType"],
            len(payload),
        )
        check_current()
        stored = self.registry.put_manifest(
            self.repository, reference.tag, payload, media_type=reference.media_type
        )
        if stored != digest:
            raise ValueError("registry stored another checkpoint manifest")
        check_current()
        return reference

    def _manifest(self, reference):
        if reference.repository != self.repository:
            raise ValueError("checkpoint belongs to another configured repository")
        document, _ = self.registry.manifest_document(
            self.repository, reference.manifest_digest
        )
        payload = canonical_bytes(document)
        if (
            len(payload) != reference.size
            or digest_bytes(payload) != reference.manifest_digest
        ):
            raise ValueError(
                "checkpoint manifest does not match its immutable reference"
            )
        return document

    def verify_memory(self, publication):
        manifest = self._manifest(publication.reference)
        config = manifest.get("config", {})
        payload = self.registry.blob_bytes(
            self.repository, config.get("digest", ""), max_bytes=MAX_HEADER
        )
        expected = {
            "schema": MEMORY_SCHEMA,
            "source_manifest_sha256": publication.source_manifest_sha256,
            "files": [file.to_dict() for file in publication.files],
        }
        if (
            payload != canonical_bytes(expected)
            or config.get("digest") != digest_bytes(payload)
            or config.get("size") != len(payload)
        ):
            raise ValueError("checkpoint memory config changed")
        layers = [
            {"mediaType": MEMORY_LAYER, "digest": file.digest, "size": file.size}
            for file in publication.files
        ]
        if manifest.get("layers") != layers:
            raise ValueError("checkpoint memory layer inventory changed")
        return publication

    def restore_memory(
        self, publication, destination: Path, *, allowed_files, check_current
    ):
        """Download into an unadvertised generation, atomically expose it last."""
        self.verify_memory(publication)
        expected = {file.name: file for file in publication.files}
        if set(expected) != set(allowed_files):
            raise ValueError("checkpoint memory publication has another file inventory")
        if (
            not destination.is_absolute()
            or destination.exists()
            or destination.is_symlink()
        ):
            raise ValueError(
                "checkpoint import destination must be absent and absolute"
            )
        staging = destination.with_name("." + destination.name + ".importing")
        if staging.is_symlink():
            raise ValueError("checkpoint staging cannot be a symlink")
        # This name belongs to the destination's fenced import operation; no
        # running or COMPLETE local generation ever refers to these partial files.
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(mode=0o700)
        try:
            for file in publication.files:
                check_current()
                with self.registry.open_blob(self.repository, file.digest) as stream:
                    self._restore_blob(stream, file, staging / file.name, check_current)
            check_current()
            fd = os.open(staging, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            staging.rename(destination)
            fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise

    @staticmethod
    def _restore_blob(stream, file, destination, check_current):
        digest, consumed = hashlib.sha256(), 0

        def read_exact(size):
            nonlocal consumed
            chunks, remaining = [], size
            while remaining:
                check_current()
                chunk = stream.read(min(CHUNK_BYTES, remaining))
                if not chunk:
                    raise ValueError("checkpoint memory blob ended early")
                chunks.append(chunk)
                remaining -= len(chunk)
                consumed += len(chunk)
                if consumed > file.size:
                    raise ValueError("checkpoint memory blob exceeds descriptor")
                digest.update(chunk)
            return b"".join(chunks)

        length = struct.unpack(">I", read_exact(4))[0]
        if not 0 < length <= MAX_HEADER:
            raise ValueError("invalid sparse memory header length")
        header = json.loads(read_exact(length))
        if (
            not isinstance(header, dict)
            or set(header) != {"name", "logical_bytes", "extents"}
            or header["name"] != file.name
            or type(header["logical_bytes"]) is not int
            or header["logical_bytes"] != file.logical_bytes
            or not isinstance(header["extents"], list)
        ):
            raise ValueError("sparse memory header does not match descriptor")
        extents, previous, payload_size = [], 0, 0
        for extent in header["extents"]:
            if (
                not isinstance(extent, list)
                or len(extent) != 2
                or any(type(value) is not int for value in extent)
            ):
                raise ValueError("invalid sparse memory extent")
            start, size = extent
            if start < previous or size <= 0 or start + size > file.logical_bytes:
                raise ValueError("sparse memory extents overlap or exceed file")
            extents.append((start, size))
            previous, payload_size = start + size, payload_size + size
        if consumed + payload_size != file.size:
            raise ValueError("sparse memory extents do not match encoded size")
        fd = os.open(
            destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        )
        try:
            os.ftruncate(fd, file.logical_bytes)
            for start, size in extents:
                offset = start
                while offset < start + size:
                    chunk = read_exact(min(CHUNK_BYTES, start + size - offset))
                    written = 0
                    while written < len(chunk):
                        count = os.pwrite(fd, chunk[written:], offset + written)
                        if count <= 0:
                            raise OSError("checkpoint memory write made no progress")
                        written += count
                    offset += len(chunk)
            if stream.read(1) or "sha256:" + digest.hexdigest() != file.digest:
                raise ValueError("checkpoint memory blob digest changed")
            check_current()
            os.fsync(fd)
        finally:
            os.close(fd)

    def publish_root(self, workspace, memory, *, portable_manifest, check_current):
        if workspace.backend != "registry" or workspace.repository != self.repository:
            raise ValueError(
                "split checkpoint requires one configured Registry repository"
            )
        document, _ = self.registry.manifest_document(
            self.repository, workspace.manifest_digest
        )
        workspace_ref = CheckpointReference(
            workspace.repository,
            workspace.tag,
            workspace.manifest_digest,
            OCI_IMAGE,
            len(canonical_bytes(document)),
        )
        self._manifest(workspace_ref)
        self.verify_memory(memory)
        root = self._root_document(workspace_ref, memory.reference, portable_manifest)
        return self._put_manifest(
            root, prefix="checkpoint", check_current=check_current
        )

    @staticmethod
    def _root_document(workspace, memory, portable_manifest):
        return {
            "schemaVersion": 2,
            "mediaType": OCI_INDEX,
            "manifests": [
                workspace.descriptor("workspace"),
                memory.descriptor("memory"),
            ],
            "annotations": {
                "org.ucloud.checkpoint.schema": ROOT_SCHEMA,
                "org.ucloud.checkpoint.manifest": digest_bytes(
                    canonical_bytes(portable_manifest)
                ),
            },
        }

    def verify_root(self, root, workspace, memory, *, portable_manifest):
        document = self._manifest(root)
        workspace_document, _ = self.registry.manifest_document(
            self.repository, workspace.manifest_digest
        )
        workspace_ref = CheckpointReference(
            workspace.repository,
            workspace.tag,
            workspace.manifest_digest,
            OCI_IMAGE,
            len(canonical_bytes(workspace_document)),
        )
        self._manifest(workspace_ref)
        self.verify_memory(memory)
        if document != self._root_document(
            workspace_ref, memory.reference, portable_manifest
        ):
            raise ValueError("checkpoint root does not bind both exact components")
        return root
