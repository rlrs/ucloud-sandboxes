from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from .direct_oci import DirectOciConfigBuilder
from .direct_provisioner import DirectSandboxProvisioner
from .direct_registry import DirectSandboxRegistry
from .direct_service import DirectSandboxService
from .direct_warden import DirectRunscWarden, DirectRunscWardenConfig
from .hibernation import HibernationDiskLedger, HibernationRuntimeFingerprint
from .image_rootfs import DockerRootfsStore, OverlayRootfsManager
from .runtime_identity import NodeRuntimeIdentityStore
from .sandbox import HibernationQuotaHelperClient


def build_direct_runtime_service(
    *,
    state_root: Path,
    quota_root: Path,
    runsc: Path,
    runsc_commit: str,
    init_binary: Path,
    disk_capacity_mb: int,
    disk_headroom_mb: int,
    quota_helper: Path = Path(
        "/usr/local/libexec/ucloud-sandbox-hibernation-quota"
    ),
    docker_binary: str = "docker",
    network: str = "none",
    max_concurrent_restores: int = 8,
    idle_park_seconds: float = 1.0,
) -> DirectSandboxService:
    """Assemble the one production direct-runtime owner for an entire node."""
    for label, path in (
        ("state_root", state_root),
        ("quota_root", quota_root),
        ("runsc", runsc),
        ("init_binary", init_binary),
        ("quota_helper", quota_helper),
    ):
        if not path.is_absolute():
            raise ValueError(f"{label} must be absolute")
    if network != "none":
        raise ValueError("only the qualified direct network=none mode is available")
    state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    quota_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    runsc_digest = _sha256_file(runsc)
    boot_digest = _canonical_sha256(
        {
            "network": network,
            "platform": "systrap",
            "remove_memory_directory_on_delete": False,
            "restore_background": True,
            "restore_cpu_startup_burst": True,
            "rootfs_format": "docker-export-overlay-v2",
            "quota_layout": "unified-xfs-project-v1",
        }
    )
    fingerprint = HibernationRuntimeFingerprint(
        runsc_sha256=runsc_digest,
        runsc_commit=runsc_commit,
        platform="systrap",
        architecture=os.uname().machine,
        page_size=os.sysconf("SC_PAGE_SIZE"),
        cpu_features_sha256=_cpu_features_sha256(),
        boot_config_sha256=boot_digest,
        # Replaced with the exact image identity for every artifact manifest.
        rootfs_sha256="0" * 64,
    )
    image_store = DockerRootfsStore(
        state_root / "image-cache",
        docker_binary=docker_binary,
    )
    overlays = OverlayRootfsManager(
        image_store,
        writable_root=quota_root,
        bundle_root=state_root / "bundles",
        require_precreated_writable=True,
    )
    warden = DirectRunscWarden(
        DirectRunscWardenConfig(
            runsc=runsc,
            runtime_root=state_root / "runsc",
            memory_root=quota_root,
            bundle_root=state_root / "bundles",
            journal_root=state_root / "journals",
            artifact_root=quota_root,
            runtime_fingerprint=fingerprint,
            network=network,
            restore_background=True,
            restore_cpu_startup_burst=True,
            remove_memory_directory_on_delete=False,
        )
    )
    provisioner = DirectSandboxProvisioner(
        identity_store=NodeRuntimeIdentityStore(state_root / "runtime-identity.json"),
        registry=DirectSandboxRegistry(state_root / "direct-registry.json"),
        disk_ledger=HibernationDiskLedger(
            state_root / "disk-ledger.json",
            capacity_mb=disk_capacity_mb,
            safety_headroom_mb=disk_headroom_mb,
        ),
        quota_backend=HibernationQuotaHelperClient(
            helper=str(quota_helper),
            sudo=True,
            include_writable_disk=True,
        ),
        image_store=image_store,
        overlays=overlays,
        oci=DirectOciConfigBuilder(init_binary=init_binary),
        warden=warden,
    )
    return DirectSandboxService(
        provisioner,
        max_concurrent_restores=max_concurrent_restores,
        idle_park_seconds=idle_park_seconds,
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _canonical_sha256(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()


def _cpu_features_sha256() -> str:
    try:
        lines = Path("/proc/cpuinfo").read_text(encoding="ascii").splitlines()
        features = next(line for line in lines if line.startswith(("flags", "Features")))
    except (OSError, StopIteration) as exc:
        raise ValueError("direct runtime requires a stable /proc/cpuinfo feature set") from exc
    return hashlib.sha256(features.encode("ascii")).hexdigest()
