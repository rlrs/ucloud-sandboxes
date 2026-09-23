"""Component identities for a split execution checkpoint.

Local paths and project-quota numbers are resolved by their owning stores. They
are never portable checkpoint identity or independent execution authority.
"""

from dataclasses import asdict, dataclass
import re

_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,239}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class WorkspaceCaptureRef:
    volume_id: str
    capture_id: str

    def __post_init__(self):
        if not isinstance(self.volume_id, str) or not _ID.fullmatch(self.volume_id):
            raise ValueError("invalid workspace volume identity")
        if not isinstance(self.capture_id, str) or not _DIGEST.fullmatch(
            self.capture_id
        ):
            raise ValueError("invalid workspace capture identity")

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) != {"volume_id", "capture_id"}:
            raise ValueError("invalid workspace capture schema")
        return cls(**raw)


@dataclass(frozen=True)
class MemoryBackingRef:
    allocation_id: str
    quota_bytes: int

    def __post_init__(self):
        if not isinstance(self.allocation_id, str) or not _ID.fullmatch(
            self.allocation_id
        ):
            raise ValueError("invalid memory allocation identity")
        if type(self.quota_bytes) is not int or self.quota_bytes <= 0:
            raise ValueError("invalid memory allocation quota")

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) != {"allocation_id", "quota_bytes"}:
            raise ValueError("invalid memory allocation schema")
        return cls(**raw)
