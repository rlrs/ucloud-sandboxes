"""Signed metadata prefetch hint of one immutable environment component (C2.2).

Representation. Workers parse the signed component config with an exact key
set and the component manifest with an exact ``layers`` list, so a new config
field or layer would make every old worker refuse the component. They ignore
manifest annotations, which the manifest digest still binds. The hint is
therefore a canonical JSON annotation on the component manifest, separately
signed by the same producer and bound to the signed config digest. It adds
no blob, so OCI blob collection and owner leases are unchanged, and a builder
reusing a tagged component keeps whatever annotation it carries.

A hint lists the 256 KiB chunks holding EROFS metadata with the metadata
bytes in each. It is only a prefetch order: every chunk a worker fetches is
still verified against the signed index, and an absent, unsupported or
unverifiable hint means demand loading, never a failed attach.
"""
from dataclasses import dataclass
import base64
import json
import logging

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from .environment_artifact import CHUNK_BYTES, canonical_bytes, content_digest, require_digest

_LOG = logging.getLogger(__name__)
METADATA_ANNOTATION = "org.ucloud.environment.metadata.v1"
METADATA_SCHEMA = "ucloud-environment-metadata-hint-v1"
WALKER = "erofs-metadata-v1"
_DOMAIN = b"ucloud.immutable-environment-metadata.v1\0"
# Bounds the manifest: 16,384 chunks is 4 GiB of metadata-bearing chunks.
MAX_HINT_CHUNKS = 16384
MAX_ANNOTATION_BYTES = 512 * 1024
_FIELDS = {"schema", "walker", "component", "image_digest", "chunk_count", "chunks",
           "metadata_bytes", "complete", "producer_key", "signature"}


@dataclass(frozen=True)
class MetadataHint:
    component: str  # Digest of the signed component config (its chunk index).
    image_digest: str
    chunk_count: int
    chunks: tuple[tuple[int, int], ...]  # (chunk index, metadata bytes), increasing index.
    complete: bool  # False when the densest MAX_HINT_CHUNKS were kept.
    producer_key: str
    signature: str = ""
    walker: str = WALKER
    schema: str = METADATA_SCHEMA

    def __post_init__(self):
        require_digest(self.component)
        require_digest(self.image_digest)
        require_digest(self.producer_key)
        if self.schema != METADATA_SCHEMA or self.walker != WALKER or type(self.complete) is not bool:
            raise ValueError("unsupported environment metadata hint")
        if (type(self.chunk_count) is not int or not 0 < self.chunk_count <= 65536
                or not isinstance(self.chunks, tuple) or not 0 < len(self.chunks) <= MAX_HINT_CHUNKS):
            raise ValueError("invalid environment metadata hint bounds")
        previous = -1
        for item in self.chunks:
            if (not isinstance(item, tuple) or len(item) != 2 or any(type(value) is not int for value in item)
                    or not previous < item[0] < self.chunk_count or not 0 < item[1] <= CHUNK_BYTES):
                raise ValueError("invalid environment metadata hint chunk")
            previous = item[0]

    @property
    def metadata_bytes(self):
        return sum(size for _, size in self.chunks)

    def unsigned(self):
        return {"schema": self.schema, "walker": self.walker, "component": self.component,
                "image_digest": self.image_digest, "chunk_count": self.chunk_count,
                "chunks": [list(item) for item in self.chunks], "metadata_bytes": self.metadata_bytes,
                "complete": self.complete, "producer_key": self.producer_key}

    def encode(self):
        return canonical_bytes(self.unsigned() | {"signature": self.signature}).decode("ascii")

    @classmethod
    def decode(cls, value):
        if not isinstance(value, str) or len(value) > MAX_ANNOTATION_BYTES:
            raise ValueError("invalid environment metadata hint encoding")
        raw = json.loads(value)
        if not isinstance(raw, dict) or set(raw) != _FIELDS or not isinstance(raw["chunks"], list):
            raise ValueError("invalid environment metadata hint fields")
        chunks = tuple(tuple(item) if isinstance(item, list) else item for item in raw["chunks"])
        hint = cls(raw["component"], raw["image_digest"], raw["chunk_count"], chunks, raw["complete"],
                   raw["producer_key"], raw["signature"], raw["walker"], raw["schema"])
        if raw["metadata_bytes"] != hint.metadata_bytes:
            raise ValueError("environment metadata hint byte count differs")
        return hint

    def authenticate(self, trusted_keys, *, component_digest, component):
        """Verify the producer and binding to exactly this signed index."""
        key = trusted_keys.get(self.producer_key)
        if key is None or content_digest(key) != self.producer_key or self.producer_key != component.producer_key:
            raise ValueError("environment metadata hint producer is not trusted")
        if (self.component != component_digest or self.image_digest != component.image_digest
                or self.chunk_count != len(component.chunks)):
            raise ValueError("environment metadata hint describes another component")
        try:
            Ed25519PublicKey.from_public_bytes(key).verify(
                base64.b64decode(self.signature, validate=True), _DOMAIN + canonical_bytes(self.unsigned()))
        except (ValueError, TypeError, InvalidSignature) as exc:
            raise ValueError("environment metadata hint signature did not verify") from exc
        return self

    def prefetch_order(self, *, max_bytes, max_chunks):
        """Densest chunks within the budget, returned in index order.

        The block 0 chunk (superblock) is always first: mounting reads it.
        """
        ranked = sorted(self.chunks, key=lambda item: (item[0] != 0, -item[1], item[0]))
        chosen = ranked[:max(0, min(max_chunks, max_bytes // CHUNK_BYTES))]
        return tuple(sorted(index for index, _ in chosen))


def component_digest(component):
    """Digest of the canonical signed config the publisher uploads."""
    return content_digest(canonical_bytes(component.to_dict()))


def sign_hint(component, chunks, signing_key):
    """Sign (chunk index, metadata bytes) pairs, keeping the densest that fit."""
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    chunks = tuple(sorted(chunks))
    complete = len(chunks) <= MAX_HINT_CHUNKS
    if not complete:
        chunks = tuple(sorted(sorted(chunks, key=lambda item: (item[0] != 0, -item[1], item[0]))[:MAX_HINT_CHUNKS]))
    key = content_digest(signing_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw))
    hint = MetadataHint(component_digest(component), component.image_digest, len(component.chunks),
                        chunks, complete, key)
    signature = base64.b64encode(signing_key.sign(_DOMAIN + canonical_bytes(hint.unsigned()))).decode("ascii")
    return MetadataHint(hint.component, hint.image_digest, hint.chunk_count, chunks, complete, key, signature)


def sign_metadata_hint(image, component, signing_key, *, check=lambda: None):
    """Walk the published EROFS bytes; raises UnsupportedErofs when not qualified."""
    from .erofs_metadata import metadata_ranges
    metadata = metadata_ranges(image, check=check)
    if metadata.image_bytes > component.image_size:
        raise ValueError("EROFS metadata exceeds the signed image")
    return sign_hint(component, metadata.chunk_bytes(CHUNK_BYTES), signing_key), metadata


def read_metadata_hint(document, component_digest_, component, trusted_keys):
    """("present", hint), ("absent", None) or ("unsupported", None); never raises."""
    annotations = document.get("annotations")
    value = annotations.get(METADATA_ANNOTATION) if isinstance(annotations, dict) else None
    if value is None:
        return "absent", None
    try:
        return "present", MetadataHint.decode(value).authenticate(
            trusted_keys, component_digest=component_digest_, component=component)
    except (ValueError, TypeError, RecursionError) as exc:
        _LOG.warning("ignoring environment metadata hint of %s: %s", component_digest_, exc)
        return "unsupported", None
