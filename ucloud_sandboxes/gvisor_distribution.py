"""Validate the complete, pinned gVisor executable distribution before staging."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import stat


GVISOR_COMMIT = "50e1502a95d36ad2faf2c7ef33b8bf21fe975293"
GVISOR_SIDECARS = (
    "checkpointgofer",
    "gvisor-sentry-prewarmer",
    "gvisor_sentry",
    "runsc-metric-server",
)


def distribution_files(runsc: Path, commit: str) -> list[tuple[Path, str]]:
    """Return verified sidecars for the new pin; legacy deployments stay readable.

    The manifest is provenance inside the deployment's trusted input, not a
    signature. The node bundle digest authenticates the resulting distribution.
    """
    manifest_path = runsc.parent / "build-manifest.json"
    sidecar_dir = runsc.parent / "gvisor-bin"
    if commit != GVISOR_COMMIT and not manifest_path.exists():
        if sidecar_dir.exists():
            raise ValueError("gVisor companion binaries require a build manifest")
        return []
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema") != 2 or manifest.get("gvisor_commit") != commit:
        raise ValueError("gVisor distribution manifest does not match the runtime pin")
    entries = manifest.get("files")
    expected = {"runsc", *(f"gvisor-bin/{name}" for name in GVISOR_SIDECARS)}
    if not isinstance(entries, dict) or set(entries) != expected:
        raise ValueError("gVisor distribution has an incomplete executable manifest")
    if sidecar_dir.is_symlink() or not sidecar_dir.is_dir():
        raise ValueError("gVisor companion directory must be a real directory")
    if {p.name for p in sidecar_dir.iterdir()} != set(GVISOR_SIDECARS):
        raise ValueError("gVisor companion executable set does not match the manifest")
    result = []
    for relative in sorted(expected):
        path = runsc if relative == "runsc" else runsc.parent / relative
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or not info.st_mode & 0o111:
            raise ValueError(
                f"gVisor executable must be a regular executable: {relative}"
            )
        entry = entries[relative]
        if not isinstance(entry, dict) or entry.get("size") != info.st_size:
            raise ValueError(f"gVisor executable size mismatch: {relative}")
        value = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                value.update(chunk)
        digest = value.hexdigest()
        if entry.get("sha256") != digest:
            raise ValueError(f"gVisor executable digest mismatch: {relative}")
        if relative != "runsc":
            result.append((path, relative))
    return result


def installed_sidecar_fingerprints(runsc: Path, commit: str) -> dict[str, str]:
    """Fence checkpoints against the installed companions, not only runsc.

    Bootstrap has already authenticated the node bundle. Hash actual installed
    bytes here so a different companion cannot reuse an old boot fingerprint.
    """
    directory = runsc.parent / "gvisor-bin"
    if commit != GVISOR_COMMIT:
        if directory.exists():
            raise ValueError("legacy gVisor runtime must not share companion binaries")
        return {}
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("installed gVisor companion directory is missing or linked")
    if {path.name for path in directory.iterdir()} != set(GVISOR_SIDECARS):
        raise ValueError("installed gVisor companion executable set mismatch")
    result = {}
    for name in GVISOR_SIDECARS:
        path = directory / name
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or not info.st_mode & 0o111:
            raise ValueError(f"invalid installed gVisor companion: {name}")
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        result[name] = digest.hexdigest()
    return result
