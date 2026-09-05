from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Sequence

from .direct_network import DirectNetworkManager
from .direct_oci import DirectOciConfigBuilder
from .direct_provisioner import DirectSandboxProvisioner
from .direct_registry import DirectSandboxRegistry
from .direct_service import DirectSandboxService
from .direct_warden import DirectRunscWarden, DirectRunscWardenConfig
from .hibernation import HibernationRuntimeFingerprint
from .gvisor_distribution import installed_sidecar_fingerprints
from .image_rootfs import DockerOverlay2RootfsStore, OverlayRootfsManager
from .storage_native_daemon import StorageNativeNodeClient
from .telemetry import Telemetry


def build_direct_runtime_service(
    *,
    state_root: Path,
    image_cache_root: Path | None = None,
    volume_mount_root: Path,
    runsc: Path,
    runsc_commit: str,
    init_binary: Path,
    managed_init_binary: Path | None = None,
    docker_binary: str = "docker",
    network: str = "none",
    network_allow_tcp: Sequence[str] = (),
    max_concurrent_restores: int = 8,
    idle_park_seconds: float = 0.0,
    storage_native_socket: Path,
    telemetry: Telemetry | None = None,
) -> DirectSandboxService:
    """Assemble the one production direct-runtime owner for an entire node."""
    for label, path in (
        ("state_root", state_root),
        ("image_cache_root", image_cache_root or state_root / "image-cache"),
        ("volume_mount_root", volume_mount_root),
        ("runsc", runsc),
        ("init_binary", init_binary),
        *(
            (("managed_init_binary", managed_init_binary),)
            if managed_init_binary is not None
            else ()
        ),
    ):
        if not path.is_absolute():
            raise ValueError(f"{label} must be absolute")
    if network not in {"none", "sandbox"}:
        raise ValueError("direct runtime network must be none or sandbox")
    if not storage_native_socket.is_absolute():
        raise ValueError("storage_native_socket must be absolute")
    state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    resolved_image_cache_root = image_cache_root or state_root / "image-cache"
    resolved_image_cache_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    volume_mount_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    runsc_digest = _sha256_file(runsc)
    boot_settings: dict[str, object] = {
        "network": network,
        "platform": "systrap",
        "rootfs_format": "ucloud-overlay2-rootfs-v1",
        "quota_layout": "storage-native-v1",
    }
    companions = installed_sidecar_fingerprints(runsc, runsc_commit)
    if companions:
        # Preserve legacy fingerprints, but bind new checkpoints to every
        # executable that can implement their kernel and restore operations.
        boot_settings["gvisor_companions"] = companions
    boot_digest = _canonical_sha256(boot_settings)
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
    image_store = DockerOverlay2RootfsStore(
        resolved_image_cache_root,
        docker_binary=docker_binary,
    )
    overlays = OverlayRootfsManager(
        image_store,
        writable_root=volume_mount_root,
        bundle_root=state_root / "bundles",
        require_precreated_writable=True,
    )
    network_manager = (
        DirectNetworkManager(
            state_root / "network-slots.json",
            allowed_tcp_egress=network_allow_tcp,
        )
        if network == "sandbox"
        else None
    )
    storage_client = StorageNativeNodeClient(
        storage_native_socket,
        telemetry=telemetry,
    )
    storage_client.wait_ready()
    warden = DirectRunscWarden(
        DirectRunscWardenConfig(
            runsc=runsc,
            runtime_root=state_root / "runsc",
            memory_root=volume_mount_root,
            bundle_root=state_root / "bundles",
            journal_root=state_root / "journals",
            runtime_fingerprint=fingerprint,
            network=network,
        ),
        storage=storage_client,
        rootfs_lifecycle=overlays,
        telemetry=telemetry,
    )
    provisioner = DirectSandboxProvisioner(
        registry=DirectSandboxRegistry(state_root / "direct-registry.sqlite"),
        image_store=image_store,
        overlays=overlays,
        oci=DirectOciConfigBuilder(
            init_binary=init_binary,
            managed_init_binary=managed_init_binary,
            network_mode=network,
        ),
        warden=warden,
        network_manager=network_manager,
    )
    return DirectSandboxService(
        provisioner,
        max_concurrent_restores=max_concurrent_restores,
        idle_park_seconds=idle_park_seconds,
        telemetry=telemetry,
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
        features = next(
            line for line in lines if line.startswith(("flags", "Features"))
        )
    except (OSError, StopIteration) as exc:
        raise ValueError(
            "direct runtime requires a stable /proc/cpuinfo feature set"
        ) from exc
    return hashlib.sha256(features.encode("ascii")).hexdigest()
