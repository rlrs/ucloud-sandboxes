"""Signed immutable environment components on the existing OCI registry.

Only a trusted builder signs the chunk index. Workers authenticate that index
before exposing any filesystem bytes, then verify each immutable chunk in full.
These are build artifacts, never execution snapshots or mutable volume exports.
"""
from dataclasses import dataclass
import base64
import hashlib
import json
from pathlib import Path
import re
from typing import Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from .managed_registry import RegistryClient

CHUNK_BYTES = 256 * 1024
COMPONENT_SCHEMA = "ucloud-environment-erofs-v1"
COMPONENT_MEDIA_TYPE = "application/vnd.ucloud.environment.erofs.v1+json"
CHUNK_MEDIA_TYPE = "application/vnd.ucloud.environment.chunk.v1"
OCI_IMAGE = "application/vnd.oci.image.manifest.v1+json"
MAX_INDEX_BYTES = 16 * 1024 * 1024
_MAX_CHUNKS = 65536
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_REPOSITORY = re.compile(r"[a-z0-9]+(?:[._/-][a-z0-9]+)*\Z")
_SIGNING_DOMAIN = b"ucloud.immutable-environment-component.v1\0"


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def content_digest(payload):
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def require_digest(value):
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise ValueError("immutable environment requires a SHA256 digest")
    return value


@dataclass(frozen=True)
class Chunk:
    digest: str
    size: int

    def __post_init__(self):
        require_digest(self.digest)
        if type(self.size) is not int or not 0 < self.size <= CHUNK_BYTES:
            raise ValueError("invalid environment chunk size")

    def to_dict(self):
        return {"digest": self.digest, "size": self.size}

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) != {"digest", "size"}:
            raise ValueError("invalid environment chunk descriptor")
        return cls(**raw)


@dataclass(frozen=True)
class EnvironmentComponent:
    source_image: str
    image_digest: str
    image_size: int
    chunks: tuple[Chunk, ...]
    producer_key: str
    signature: str
    schema: str = COMPONENT_SCHEMA
    filesystem: str = "erofs-host-v1"
    source_kind: str = "fresh-allowlisted-build-v1"

    def __post_init__(self):
        require_digest(self.source_image)
        require_digest(self.image_digest)
        require_digest(self.producer_key)
        if (self.schema != COMPONENT_SCHEMA or self.filesystem != "erofs-host-v1"
                or self.source_kind != "fresh-allowlisted-build-v1"):
            raise ValueError("unqualified immutable environment format/provenance")
        if (type(self.image_size) is not int or self.image_size <= 0 or self.image_size % 4096
                or not isinstance(self.chunks, tuple) or not 0 < len(self.chunks) <= _MAX_CHUNKS
                or any(not isinstance(chunk, Chunk) for chunk in self.chunks)
                or any(chunk.size != CHUNK_BYTES for chunk in self.chunks[:-1])
                or sum(chunk.size for chunk in self.chunks) != self.image_size):
            raise ValueError("invalid authenticated environment range index")
        try:
            if len(base64.b64decode(self.signature, validate=True)) != 64:
                raise ValueError("invalid signature length")
        except (ValueError, TypeError) as exc:
            raise ValueError("invalid environment producer signature") from exc

    def unsigned(self):
        return {"schema": self.schema, "filesystem": self.filesystem,
                "source_kind": self.source_kind, "source_image": self.source_image,
                "image_digest": self.image_digest, "image_size": self.image_size,
                "chunks": [chunk.to_dict() for chunk in self.chunks],
                "producer_key": self.producer_key}

    def to_dict(self):
        return self.unsigned() | {"signature": self.signature}

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) != {
            "schema", "filesystem", "source_kind", "source_image", "image_digest",
            "image_size", "chunks", "producer_key", "signature",
        } or not isinstance(raw["chunks"], list):
            raise ValueError("invalid environment component schema")
        return cls(**(raw | {"chunks": tuple(Chunk.from_dict(chunk) for chunk in raw["chunks"])}))

    def authenticate(self, trusted_keys: Mapping[str, bytes]):
        key_bytes = trusted_keys.get(self.producer_key)
        if key_bytes is None or content_digest(key_bytes) != self.producer_key:
            raise ValueError("environment producer is not trusted")
        try:
            Ed25519PublicKey.from_public_bytes(key_bytes).verify(
                base64.b64decode(self.signature, validate=True),
                _SIGNING_DOMAIN + canonical_bytes(self.unsigned()),
            )
        except (ValueError, InvalidSignature) as exc:
            raise ValueError("environment producer signature did not verify") from exc
        return self


def sign_component(image: Path, *, source_image: str, signing_key: Ed25519PrivateKey):
    """Sign the output of the fresh builder; publication never accepts checkpoints."""
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    chunks, digest = [], hashlib.sha256()
    with image.open("rb") as source:
        while payload := source.read(CHUNK_BYTES):
            digest.update(payload)
            chunks.append(Chunk(content_digest(payload), len(payload)))
    key_id = content_digest(signing_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw))
    candidate = EnvironmentComponent(source_image, "sha256:" + digest.hexdigest(),
        sum(chunk.size for chunk in chunks), tuple(chunks), key_id, base64.b64encode(bytes(64)).decode("ascii"))
    signature = signing_key.sign(_SIGNING_DOMAIN + canonical_bytes(candidate.unsigned()))
    return EnvironmentComponent.from_dict(candidate.to_dict() | {"signature": base64.b64encode(signature).decode("ascii")})


def _upload_blob(client, repository, payload, expected_digest):
    if content_digest(payload) != expected_digest:
        raise ValueError("environment changed after signing")
    if client.blob_exists(repository, expected_digest):
        return
    location = client.start_blob_upload(repository)
    try:
        location = client.upload_blob_chunk(location, payload)
        client.finish_blob_upload(location, expected_digest)
    except BaseException:
        client.abort_blob_upload(location)
        raise


class EnvironmentArtifactRegistry:
    """Immutable component publication and trusted lookup; no parallel catalog."""
    def __init__(self, client: RegistryClient, repository: str, trusted_keys: Mapping[str, bytes]):
        if not _REPOSITORY.fullmatch(repository):
            raise ValueError("invalid environment repository")
        self.client = client
        self.repository = repository
        self.trusted_keys = dict(trusted_keys)

    def publish(self, image: Path, component: EnvironmentComponent, *, tag: str) -> str:
        component.authenticate(self.trusted_keys)
        with image.open("rb") as source:
            for chunk in component.chunks:
                payload = source.read(chunk.size)
                if len(payload) != chunk.size:
                    raise ValueError("environment image truncated after signing")
                _upload_blob(self.client, self.repository, payload, chunk.digest)
            if source.read(1):
                raise ValueError("environment image grew after signing")
        config = canonical_bytes(component.to_dict())
        config_digest = content_digest(config)
        _upload_blob(self.client, self.repository, config, config_digest)
        manifest = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
            "config": {"mediaType": COMPONENT_MEDIA_TYPE, "digest": config_digest, "size": len(config)},
            "layers": [{"mediaType": CHUNK_MEDIA_TYPE, **chunk.to_dict()} for chunk in component.chunks]})
        # The root is the commit point; interrupted chunk uploads are never a
        # partially visible environment and follow ordinary registry blob GC.
        self.client.put_manifest(self.repository, tag, manifest, media_type=OCI_IMAGE)
        return content_digest(manifest)

    def load(self, digest: str) -> EnvironmentComponent:
        require_digest(digest)
        document, _headers = self.client.manifest_document(self.repository, digest)
        # The producer signature authenticates the config even if registry JSON
        # formatting differs; additionally bind every executable chunk to OCI GC.
        config = document.get("config", {})
        if (document.get("schemaVersion") != 2 or document.get("mediaType") != OCI_IMAGE
                or not isinstance(config, dict) or config.get("mediaType") != COMPONENT_MEDIA_TYPE
                or type(config.get("size")) is not int or not 0 < config["size"] <= MAX_INDEX_BYTES):
            raise ValueError("invalid environment OCI metadata")
        require_digest(config.get("digest"))
        payload = self.client.blob_bytes(self.repository, config["digest"], max_bytes=config["size"])
        if len(payload) != config["size"] or content_digest(payload) != config["digest"]:
            raise ValueError("environment index content identity mismatch")
        component = EnvironmentComponent.from_dict(json.loads(payload)).authenticate(self.trusted_keys)
        expected = [{"mediaType": CHUNK_MEDIA_TYPE, **chunk.to_dict()} for chunk in component.chunks]
        if document.get("layers") != expected:
            raise ValueError("environment OCI dependency closure differs from signed index")
        # Publisher uses canonical JSON, so an attacker cannot substitute a
        # differently signed component for the selected immutable root digest.
        if content_digest(canonical_bytes(document)) != digest:
            raise ValueError("environment manifest content identity mismatch")
        return component

ENVIRONMENT_ANNOTATION = "org.ucloud.immutable-environment.v1"
ENVIRONMENT_MEDIA_TYPE = "application/vnd.ucloud.environment.v1+json"
_ENVIRONMENT_DOMAIN = b"ucloud.immutable-environment.v1\0"


@dataclass(frozen=True)
class ImmutableEnvironment:
    source_image: str
    environment: object
    image_config: dict
    producer_key: str
    signature: str

    def unsigned(self):
        return {"schema": "ucloud-immutable-environment-v1", "source_image": self.source_image,
                "environment": self.environment.to_dict(), "image_config": self.image_config,
                "producer_key": self.producer_key}

    def to_dict(self):
        return self.unsigned() | {"signature": self.signature}

    @property
    def components(self):
        return (self.environment.base,
                *((self.environment.workspace,) if self.environment.workspace is not None else ()),
                *self.environment.toolkits)

    @classmethod
    def from_dict(cls, raw):
        from .environment_manifest import EnvironmentManifest
        from .image_rootfs import DockerImageConfig
        if (not isinstance(raw, dict) or set(raw) != {
                "schema", "source_image", "environment", "image_config", "producer_key", "signature"}
                or raw["schema"] != "ucloud-immutable-environment-v1"):
            raise ValueError("invalid immutable environment metadata")
        require_digest(raw["source_image"])
        require_digest(raw["producer_key"])
        DockerImageConfig.from_inspection(raw["image_config"])
        environment = EnvironmentManifest.from_dict(raw["environment"])
        if len(environment.toolkits) > 32:
            raise ValueError("environment component count exceeds mount bound")
        return cls(raw["source_image"], environment, raw["image_config"], raw["producer_key"], raw["signature"])

    def authenticate(self, trusted_keys):
        key = trusted_keys.get(self.producer_key)
        if key is None or content_digest(key) != self.producer_key:
            raise ValueError("environment producer is not trusted")
        try:
            signature = base64.b64decode(self.signature, validate=True)
            Ed25519PublicKey.from_public_bytes(key).verify(signature, _ENVIRONMENT_DOMAIN + canonical_bytes(self.unsigned()))
        except (ValueError, TypeError, InvalidSignature) as exc:
            raise ValueError("environment composition signature did not verify") from exc
        return self


def publish_environment(registry, *, source_image, environment, image_config, signing_key, tag):
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    key = signing_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    raw = {"schema": "ucloud-immutable-environment-v1", "source_image": source_image,
           "environment": environment.to_dict(), "image_config": image_config,
           "producer_key": content_digest(key), "signature": ""}
    candidate = ImmutableEnvironment.from_dict(raw)
    signed = ImmutableEnvironment.from_dict(raw | {"signature": base64.b64encode(signing_key.sign(
        _ENVIRONMENT_DOMAIN + canonical_bytes(candidate.unsigned()))).decode("ascii")})
    signed.authenticate(registry.trusted_keys)
    # Refuse missing/untrusted components before committing composition. Base
    # must originate from exactly this OCI config; toolkits carry their own
    # independently signed source identities.
    for index, digest in enumerate(signed.components):
        component = registry.load(digest)
        if index == 0 and component.source_image != source_image:
            raise ValueError("environment base belongs to a different OCI image")
    config = canonical_bytes(signed.to_dict())
    digest = content_digest(config)
    _upload_blob(registry.client, registry.repository, config, digest)
    manifest = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
        "config": {"mediaType": ENVIRONMENT_MEDIA_TYPE, "digest": digest, "size": len(config)},
        "layers": []})
    registry.client.put_manifest(registry.repository, tag, manifest, media_type=OCI_IMAGE)
    return content_digest(manifest)


def environment_root_digest(environment):
    config = canonical_bytes(environment.to_dict())
    return content_digest(canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
        "config": {"mediaType": ENVIRONMENT_MEDIA_TYPE, "digest": content_digest(config), "size": len(config)},
        "layers": []}))


def load_environment(registry, digest):
    require_digest(digest)
    document, _ = registry.client.manifest_document(registry.repository, digest)
    if content_digest(canonical_bytes(document)) != digest:
        raise ValueError("environment root content identity mismatch")
    config = document.get("config", {})
    if (document.get("schemaVersion") != 2 or document.get("mediaType") != OCI_IMAGE
            or document.get("layers") != [] or not isinstance(config, dict)
            or config.get("mediaType") != ENVIRONMENT_MEDIA_TYPE
            or type(config.get("size")) is not int or not 0 < config["size"] <= MAX_INDEX_BYTES):
        raise ValueError("invalid immutable environment root")
    require_digest(config.get("digest"))
    data = registry.client.blob_bytes(registry.repository, config["digest"], max_bytes=config["size"])
    if len(data) != config["size"] or content_digest(data) != config["digest"]:
        raise ValueError("environment root metadata identity mismatch")
    return ImmutableEnvironment.from_dict(json.loads(data)).authenticate(registry.trusted_keys)


def attach_environment_to_image(registry, *, image_repository, image_reference, environment_digest):
    """Add metadata to the existing OCI image; Docker's config/layers stay intact."""
    environment = load_environment(registry, environment_digest)
    document, _ = registry.client.manifest_document(image_repository, image_reference)
    if document.get("config", {}).get("digest") != environment.source_image:
        raise ValueError("immutable environment does not match source OCI image")
    annotations = dict(document.get("annotations") or {})
    annotations[ENVIRONMENT_ANNOTATION] = environment_digest
    document = document | {"annotations": annotations}
    payload = canonical_bytes(document)
    registry.client.put_manifest(image_repository, image_reference, payload,
                                 media_type=document.get("mediaType", OCI_IMAGE))
    return content_digest(payload)


def load_image_environment(registry, repository, reference, *, required=True):
    """Resolve the one signed attachment format used by builder and workers."""
    document, _ = registry.client.manifest_document(repository, reference)
    annotations = document.get("annotations", {})
    if not isinstance(annotations, dict):
        raise ValueError("invalid image environment annotations")
    root = annotations.get(ENVIRONMENT_ANNOTATION)
    if root is None and not required:
        return None
    require_digest(root)
    # Our attachment writer always emits canonical source-image manifests.
    # Verify pinned input identity rather than trusting a registry's digest header.
    if _DIGEST.fullmatch(reference) and content_digest(canonical_bytes(document)) != reference:
        raise ValueError("annotated source image identity mismatch")
    environment = load_environment(registry, root)
    config = document.get("config")
    if not isinstance(config, dict) or config.get("digest") != environment.source_image:
        raise ValueError("signed environment belongs to another OCI image")
    return root, environment
