"""Signed immutable environment components on the existing OCI registry.

Only a trusted builder signs the chunk index. Workers authenticate that index
before exposing any filesystem bytes, then verify each immutable chunk in full.
These are build artifacts, never execution snapshots or mutable volume exports.
"""
from dataclasses import dataclass, field
import base64
import hashlib
import json
import logging
from pathlib import Path
import re
import threading
import time
from typing import Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from .managed_registry import RegistryClient, RegistryRequestError

_LOG = logging.getLogger(__name__)
_UPLOAD_ATTEMPTS = 3

CHUNK_BYTES = 256 * 1024
COMPONENT_SCHEMA = "ucloud-environment-erofs-v1"
# One EROFS component per group of OCI layers, shared by every image whose
# layers include that group on the same parent chain (docs/immutable-environments.md).
COMPONENT_SCHEMA_V2 = "ucloud-environment-erofs-v2"
LAYER_SOURCE_KIND = "oci-layer-diffs-v1"
LAYER_TAG_PREFIX = "layer-"
# The diff_id of an empty tar. Some builders emit it for metadata-only
# instructions; it changes no file, so layer components skip it.
EMPTY_LAYER_DIFF_ID = "sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"
_MAX_SOURCE_LAYERS = 128
COMPONENT_MEDIA_TYPE = "application/vnd.ucloud.environment.erofs.v1+json"
CHUNK_MEDIA_TYPE = "application/vnd.ucloud.environment.chunk.v1"
# The whole EROFS image as one blob; workers read signed chunks as byte ranges.
IMAGE_MEDIA_TYPE = "application/vnd.ucloud.environment.image.v1"
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


def _require_range_index(component):
    if (type(component.image_size) is not int or component.image_size <= 0 or component.image_size % 4096
            or not isinstance(component.chunks, tuple) or not 0 < len(component.chunks) <= _MAX_CHUNKS
            or any(not isinstance(chunk, Chunk) for chunk in component.chunks)
            or any(chunk.size != CHUNK_BYTES for chunk in component.chunks[:-1])
            or sum(chunk.size for chunk in component.chunks) != component.image_size):
        raise ValueError("invalid authenticated environment range index")
    try:
        if len(base64.b64decode(component.signature, validate=True)) != 64:
            raise ValueError("invalid signature length")
    except (ValueError, TypeError) as exc:
        raise ValueError("invalid environment producer signature") from exc


def _authenticate(component, trusted_keys):
    key_bytes = trusted_keys.get(component.producer_key)
    if key_bytes is None or content_digest(key_bytes) != component.producer_key:
        raise ValueError("environment producer is not trusted")
    try:
        # The schema is part of the signed bytes, so a v1 signature never
        # authenticates a v2 index or the reverse.
        Ed25519PublicKey.from_public_bytes(key_bytes).verify(
            base64.b64decode(component.signature, validate=True),
            _SIGNING_DOMAIN + canonical_bytes(component.unsigned()),
        )
    except (ValueError, InvalidSignature) as exc:
        raise ValueError("environment producer signature did not verify") from exc
    return component


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
        _require_range_index(self)

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
        """Parse either component schema; v1 whole-image components stay valid."""
        if isinstance(raw, dict) and raw.get("schema") == COMPONENT_SCHEMA_V2:
            return LayerEnvironmentComponent.from_dict(raw)
        if not isinstance(raw, dict) or set(raw) != {
            "schema", "filesystem", "source_kind", "source_image", "image_digest",
            "image_size", "chunks", "producer_key", "signature",
        } or not isinstance(raw["chunks"], list):
            raise ValueError("invalid environment component schema")
        return cls(**(raw | {"chunks": tuple(Chunk.from_dict(chunk) for chunk in raw["chunks"])}))

    def authenticate(self, trusted_keys: Mapping[str, bytes]):
        return _authenticate(self, trusted_keys)


def layer_chain_id(diff_ids):
    """OCI ChainID of layers listed bottom to top; None for no layers."""
    chain = None
    for diff_id in diff_ids:
        require_digest(diff_id)
        chain = diff_id if chain is None else content_digest((chain + " " + diff_id).encode("ascii"))
    return chain


def require_layer_format(value):
    if (not isinstance(value, dict) or set(value) != {"layout", "mkfs", "compression", "excludes"}
            or type(value["layout"]) is not int or value["layout"] != 1
            or not isinstance(value["mkfs"], str) or not 0 < len(value["mkfs"]) <= 256
            or not value["mkfs"].isprintable()
            or not isinstance(value["compression"], str) or len(value["compression"]) > 32
            or not isinstance(value["excludes"], list) or len(value["excludes"]) > 16
            or any(not isinstance(name, str) or not name or "/" in name for name in value["excludes"])
            or value["excludes"] != sorted(set(value["excludes"]))):
        raise ValueError("invalid environment layer format")
    return value


def layer_group_key(layer_format, parent, diff_ids):
    """Content address of one layer group's component; its tag is ``layer-<key>``.

    A squashed group drops whiteouts that hide nothing beneath it, so its bytes
    depend on the lower layers: the parent ChainID is part of the key. Images
    sharing a base share its whole chain, so this costs no base reuse.
    """
    return hashlib.sha256(canonical_bytes({
        "format": require_layer_format(layer_format), "parent": parent, "diff_ids": list(diff_ids),
    })).hexdigest()


@dataclass(frozen=True)
class LayerEnvironmentComponent:
    """EROFS of ordered OCI layer diffs, overlay whiteouts included.

    It names no source image: every image with this layer group on the same
    parent chain shares the component, its chunk cache, device and mount.
    """
    source_layers: tuple[str, ...]
    parent: str | None
    format: dict = field(hash=False)
    image_digest: str = ""
    image_size: int = 0
    chunks: tuple[Chunk, ...] = ()
    producer_key: str = ""
    signature: str = ""
    schema: str = COMPONENT_SCHEMA_V2
    filesystem: str = "erofs-host-v1"
    source_kind: str = LAYER_SOURCE_KIND

    def __post_init__(self):
        if (not isinstance(self.source_layers, tuple)
                or not 0 < len(self.source_layers) <= _MAX_SOURCE_LAYERS
                or EMPTY_LAYER_DIFF_ID in self.source_layers):
            raise ValueError("invalid environment source layers")
        for digest in self.source_layers:
            require_digest(digest)
        if self.parent is not None:
            require_digest(self.parent)
        require_layer_format(self.format)
        require_digest(self.image_digest)
        require_digest(self.producer_key)
        if (self.schema != COMPONENT_SCHEMA_V2 or self.filesystem != "erofs-host-v1"
                or self.source_kind != LAYER_SOURCE_KIND):
            raise ValueError("unqualified immutable environment format/provenance")
        _require_range_index(self)

    @property
    def group_key(self):
        return layer_group_key(self.format, self.parent, self.source_layers)

    def unsigned(self):
        return {"schema": self.schema, "filesystem": self.filesystem,
                "source_kind": self.source_kind, "source_layers": list(self.source_layers),
                "parent": self.parent, "format": self.format,
                "image_digest": self.image_digest, "image_size": self.image_size,
                "chunks": [chunk.to_dict() for chunk in self.chunks],
                "producer_key": self.producer_key}

    def to_dict(self):
        return self.unsigned() | {"signature": self.signature}

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) != {
            "schema", "filesystem", "source_kind", "source_layers", "parent", "format",
            "image_digest", "image_size", "chunks", "producer_key", "signature",
        } or not isinstance(raw["chunks"], list) or not isinstance(raw["source_layers"], list):
            raise ValueError("invalid environment component schema")
        return cls(**(raw | {"chunks": tuple(Chunk.from_dict(chunk) for chunk in raw["chunks"]),
                             "source_layers": tuple(raw["source_layers"])}))

    def authenticate(self, trusted_keys: Mapping[str, bytes]):
        return _authenticate(self, trusted_keys)


def _chunk_image(image: Path):
    chunks, digest = [], hashlib.sha256()
    with image.open("rb") as source:
        while payload := source.read(CHUNK_BYTES):
            digest.update(payload)
            chunks.append(Chunk(content_digest(payload), len(payload)))
    return tuple(chunks), "sha256:" + digest.hexdigest()


def _signed(candidate, signing_key):
    signature = signing_key.sign(_SIGNING_DOMAIN + canonical_bytes(candidate.unsigned()))
    return EnvironmentComponent.from_dict(candidate.to_dict() | {"signature": base64.b64encode(signature).decode("ascii")})


def _key_id(signing_key):
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    return content_digest(signing_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw))


_UNSIGNED = base64.b64encode(bytes(64)).decode("ascii")


def sign_component(image: Path, *, source_image: str, signing_key: Ed25519PrivateKey):
    """Sign the output of the fresh builder; publication never accepts checkpoints."""
    chunks, digest = _chunk_image(image)
    return _signed(EnvironmentComponent(source_image, digest, sum(chunk.size for chunk in chunks),
                                        chunks, _key_id(signing_key), _UNSIGNED), signing_key)


def sign_layer_component(image: Path, *, source_layers, parent, layer_format, signing_key: Ed25519PrivateKey):
    """Sign the EROFS of one layer group, built from Docker's immutable diffs."""
    chunks, digest = _chunk_image(image)
    return _signed(LayerEnvironmentComponent(tuple(source_layers), parent, layer_format, digest,
                                             sum(chunk.size for chunk in chunks), chunks,
                                             _key_id(signing_key), _UNSIGNED), signing_key)


def _transient(exc: BaseException) -> bool:
    if isinstance(exc, RegistryRequestError):
        return exc.status_code in {408, 429, 500, 502, 503, 504}
    return isinstance(exc, OSError)


def _upload_blob(client, repository, payload, expected_digest):
    if content_digest(payload) != expected_digest:
        raise ValueError("environment changed after signing")
    for attempt in range(_UPLOAD_ATTEMPTS):
        if client.blob_exists(repository, expected_digest):
            return
        location = client.start_blob_upload(repository)
        try:
            location = client.upload_blob_chunk(location, payload)
            client.finish_blob_upload(location, expected_digest)
            return
        except BaseException as exc:
            try:
                client.abort_blob_upload(location)
            except Exception as abort_error:
                # A failed cleanup must never replace the upload's own error;
                # an abandoned upload follows ordinary registry GC.
                _LOG.warning("could not abort a registry upload: %s", abort_error)
            if attempt + 1 >= _UPLOAD_ATTEMPTS or not _transient(exc):
                raise
            time.sleep(0.5 * 2 ** attempt)


class EnvironmentArtifactRegistry:
    """Immutable component publication and trusted lookup; no parallel catalog."""
    def __init__(self, client: RegistryClient, repository: str, trusted_keys: Mapping[str, bytes]):
        if not _REPOSITORY.fullmatch(repository):
            raise ValueError("invalid environment repository")
        self.client = client
        self.repository = repository
        self.trusted_keys = dict(trusted_keys)
        self._whole_images: set[str] = set()
        self._layout_guard = threading.Lock()

    def whole_image(self, component: "EnvironmentComponent") -> bool:
        """Whether ``load`` found this component published as one image blob."""
        with self._layout_guard:
            return component.image_digest in self._whole_images

    def publish(self, image: Path, component: EnvironmentComponent, *, tag: str) -> str:
        component.authenticate(self.trusted_keys)
        # One streamed upload of the whole image. A registry handles each blob
        # as a separate upload and commit; per-chunk blobs made publication of
        # a 3.5 GB image take minutes (docs/image-import.md).
        if image.stat().st_size != component.image_size:
            raise ValueError("environment image size changed after signing")
        # No local re-hash: the registry verifies the signed digest when it
        # commits the upload and rejects changed content (DIGEST_INVALID).
        if not self.client.blob_exists(self.repository, component.image_digest):
            self.client.upload_blob_file(
                self.repository, image, component.image_digest, component.image_size,
            )
        config = canonical_bytes(component.to_dict())
        config_digest = content_digest(config)
        _upload_blob(self.client, self.repository, config, config_digest)
        manifest = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
            "config": {"mediaType": COMPONENT_MEDIA_TYPE, "digest": config_digest, "size": len(config)},
            "layers": [{"mediaType": IMAGE_MEDIA_TYPE, "digest": component.image_digest,
                        "size": component.image_size}]})
        # The root is the commit point; interrupted chunk uploads are never a
        # partially visible environment and follow ordinary registry blob GC.
        self.client.put_manifest(self.repository, tag, manifest, media_type=OCI_IMAGE)
        with self._layout_guard:
            self._whole_images.add(component.image_digest)
        return content_digest(manifest)

    def load(self, digest: str) -> EnvironmentComponent:
        require_digest(digest)
        document, _headers = self.client.manifest_document(self.repository, digest)
        return self.load_document(digest, document)

    def load_document(self, digest: str, document: dict) -> EnvironmentComponent:
        """Authenticate an already-fetched manifest without reading it twice.

        This only reuses the caller's document. Config bytes are still fetched
        and authenticated, and callers retain their normal liveness/GC checks.
        """
        require_digest(digest)
        if not isinstance(document, dict):
            raise ValueError("invalid environment OCI metadata")
        # Publisher uses canonical JSON, so an attacker cannot substitute a
        # differently signed component for the selected immutable root digest.
        if content_digest(canonical_bytes(document)) != digest:
            raise ValueError("environment manifest content identity mismatch")
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
        whole_image = [{"mediaType": IMAGE_MEDIA_TYPE, "digest": component.image_digest,
                        "size": component.image_size}]
        per_chunk = [{"mediaType": CHUNK_MEDIA_TYPE, **chunk.to_dict()} for chunk in component.chunks]
        # Either layout binds every byte a worker may read to OCI GC: one image
        # blob read by signed chunk ranges, or one blob per chunk (earlier builds).
        if document.get("layers") == whole_image:
            with self._layout_guard:
                self._whole_images.add(component.image_digest)
        elif document.get("layers") != per_chunk:
            raise ValueError("environment OCI dependency closure differs from signed index")
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


def bind_source_layers(components, diff_ids):
    """Check that the layer components rebuild exactly the image layers.

    Layer components come first. Concatenated in order, their source layers
    must equal the OCI config's ``rootfs.diff_ids`` (empty-tar layers aside),
    each on its parent chain. Only whole-image toolkits, with their own signed
    source identities, may follow.
    """
    expected = [digest for digest in diff_ids if digest != EMPTY_LAYER_DIFF_ID]
    layers = [component for component in components if isinstance(component, LayerEnvironmentComponent)]
    if any(not isinstance(component, LayerEnvironmentComponent) for component in components[:len(layers)]):
        raise ValueError("environment layer components differ from the OCI image layers")
    consumed = []
    for component in layers:
        if component.parent != layer_chain_id(consumed):
            raise ValueError("environment layer component sits on another parent chain")
        consumed.extend(component.source_layers)
    if not expected or consumed != expected:
        raise ValueError("environment layer components differ from the OCI image layers")


def publish_environment(registry, *, source_image, environment, image_config, signing_key, tag,
                        source_diff_ids=None):
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    key = signing_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    raw = {"schema": "ucloud-immutable-environment-v1", "source_image": source_image,
           "environment": environment.to_dict(), "image_config": image_config,
           "producer_key": content_digest(key), "signature": ""}
    candidate = ImmutableEnvironment.from_dict(raw)
    signed = ImmutableEnvironment.from_dict(raw | {"signature": base64.b64encode(signing_key.sign(
        _ENVIRONMENT_DOMAIN + canonical_bytes(candidate.unsigned()))).decode("ascii")})
    signed.authenticate(registry.trusted_keys)
    # Refuse missing/untrusted components before committing composition. A
    # whole-image base must originate from exactly this OCI config; layer
    # components must rebuild exactly its diff_ids, which the builder read
    # under this image ID (the config digest). Toolkits carry their own
    # independently signed source identities.
    components = [registry.load(digest) for digest in signed.components]
    if isinstance(components[0], LayerEnvironmentComponent):
        if source_diff_ids is None:
            raise ValueError("layer environment components require the OCI image diff_ids")
        bind_source_layers(components, source_diff_ids)
    elif components[0].source_image != source_image:
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
