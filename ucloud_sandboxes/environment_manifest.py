"""Immutable environment identity, distinct from writable workspace revisions.

Component order is base, optional workspace seed, then toolkits in declared
order. Later components win with OCI whiteout/opaque-directory semantics.
This describes composition; it does not claim any backend can execute it.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
DOCKER_OVERLAY2_ABI = "ucloud-overlay2-rootfs-v1"
HOST_EROFS_ABI = "ucloud-host-erofs-environment-v1"


@dataclass(frozen=True)
class EnvironmentManifest:
    base: str
    workspace: str | None = None
    toolkits: tuple[str, ...] = ()
    composition: str = "oci-overlay-v1"
    schema: int = 1

    def __post_init__(self) -> None:
        if type(self.schema) is not int or self.schema != 1:
            raise ValueError("unsupported environment manifest schema")
        if self.composition != "oci-overlay-v1":
            raise ValueError("unsupported environment composition")
        if not isinstance(self.toolkits, tuple):
            raise ValueError("environment toolkits must be an immutable ordered tuple")
        for digest in (
            self.base,
            *(() if self.workspace is None else (self.workspace,)),
            *self.toolkits,
        ):
            if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
                raise ValueError(
                    "environment components require immutable sha256 digests"
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "composition": self.composition,
            "base": self.base,
            "workspace": self.workspace,
            "toolkits": list(self.toolkits),
        }

    @classmethod
    def from_dict(cls, raw: object) -> EnvironmentManifest:
        if not isinstance(raw, dict) or set(raw) != {
            "schema",
            "composition",
            "base",
            "workspace",
            "toolkits",
        }:
            raise ValueError("invalid environment manifest fields")
        if not isinstance(raw["toolkits"], list):
            raise ValueError("environment toolkits must be an ordered list")
        return cls(
            base=raw["base"],
            workspace=raw["workspace"],
            toolkits=tuple(raw["toolkits"]),
            composition=raw["composition"],
            schema=raw["schema"],
        )

    @property
    def sha256(self) -> str:
        canonical = json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        return hashlib.sha256(canonical.encode("ascii")).hexdigest()

    def rootfs_fingerprint(self, backend_abi: str) -> str:
        """Resolve only qualified ABIs, preserving existing checkpoint bytes.

        Docker currently supplies a single already-composed OCI image. The
        manifest digest is deliberately not substituted for its legacy rootfs
        fingerprint. A composed environment requires a qualified backend first.
        """
        if backend_abi == HOST_EROFS_ABI:
            return hashlib.sha256((backend_abi + "\0" + self.sha256).encode("ascii")).hexdigest()
        if backend_abi != DOCKER_OVERLAY2_ABI:
            raise ValueError("unqualified rootfs backend ABI")
        if self.workspace is not None or self.toolkits:
            raise ValueError("Docker rootfs requires one already-composed image")
        return hashlib.sha256(
            (backend_abi + "\0" + self.base).encode("ascii")
        ).hexdigest()
