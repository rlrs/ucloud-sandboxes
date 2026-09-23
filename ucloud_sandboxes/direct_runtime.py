from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Mapping, Sequence

from .checkpoint_registry import RegistryCheckpointStore
from .managed_registry import RegistryClient
from .memory_backing import MemoryBackingStore
from .direct_network import DirectNetworkManager
from .direct_oci import DirectOciConfigBuilder
from .direct_provisioner import DirectSandboxProvisioner
from .direct_registry import DirectSandboxRegistry
from .direct_service import DirectSandboxService
from .direct_warden import DirectRunscWarden, DirectRunscWardenConfig
from .hibernation import HibernationRuntimeFingerprint
from .gvisor_distribution import (
    installed_sidecar_fingerprints,
    require_capture_barrier_runtime,
    require_ram_backing_runtime,
    require_reflink_restore_runtime,
)
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
    network_relays: Mapping[str, str] | None = None,
    max_concurrent_restores: int = 8,
    max_concurrent_startups: int = 8,
    idle_park_seconds: float = 0.0,
    storage_native_socket: Path,
    split_memory_backing: bool = False,
    reflink_memory_restore: bool = False,
    application_memory_root: Path | None = None,
    memory_backing_hard_capacity_bytes: int = 0,
    checkpoint_registry_url: str = "",
    checkpoint_registry_repository: str = "",
    environment_registry: object | None = None,
    environment_backend_socket: Path | None = None,
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
    if network_relays and network != "sandbox":
        raise ValueError("network_relays requires sandbox networking")
    if network not in {"none", "sandbox"}:
        raise ValueError("direct runtime network must be none or sandbox")
    if not storage_native_socket.is_absolute():
        raise ValueError("storage_native_socket must be absolute")
    state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    resolved_image_cache_root = image_cache_root or state_root / "image-cache"
    resolved_image_cache_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    volume_mount_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if application_memory_root is not None:
        if not split_memory_backing:
            raise ValueError("RAM application memory requires split checkpoint storage")
    if not isinstance(reflink_memory_restore, bool):
        raise ValueError("reflink_memory_restore must be a boolean")
    if reflink_memory_restore and not split_memory_backing:
        raise ValueError("reflink memory restore requires split checkpoint storage")
    if split_memory_backing:
        if memory_backing_hard_capacity_bytes <= 0:
            raise ValueError(
                "split memory backing requires the shared physical disk budget"
            )
        if reflink_memory_restore:
            require_reflink_restore_runtime(runsc, runsc_commit)
        elif application_memory_root is None:
            require_capture_barrier_runtime(runsc, runsc_commit)
        else:
            require_ram_backing_runtime(runsc, runsc_commit)
    if bool(checkpoint_registry_url) != bool(checkpoint_registry_repository):
        raise ValueError(
            "checkpoint registry origin and repository must be configured together"
        )
    if (environment_registry is None) != (environment_backend_socket is None):
        raise ValueError(
            "immutable environment registry and backend socket must be selected together"
        )
    runsc_digest = _sha256_file(runsc)
    boot_settings: dict[str, object] = {
        "network": network,
        "platform": "systrap",
        "rootfs_format": (
            "ucloud-host-erofs-environment-v1"
            if environment_registry is not None
            else "ucloud-overlay2-rootfs-v1"
        ),
        "quota_layout": "split-memory-v3"
        if split_memory_backing
        else "storage-native-v1",
    }
    if reflink_memory_restore:
        # This is the portable reader capability. Fresh RAM/file placement and
        # a particular owner's later restore mode never change checkpoint ABI.
        boot_settings["application_memory_backend"] = "reflink-restore-v1"
    elif application_memory_root is not None:
        boot_settings["application_memory_backend"] = "ram-sparse-capture-v1"
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
    if environment_registry is None:
        image_store = DockerOverlay2RootfsStore(
            resolved_image_cache_root, docker_binary=docker_binary
        )
    else:
        from .environment_backend import EnvironmentBackendClient
        from .environment_rootfs import EnvironmentRootfsStore

        image_store = EnvironmentRootfsStore(
            resolved_image_cache_root,
            environment_registry,
            EnvironmentBackendClient(environment_backend_socket),
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
            network_relays=network_relays,
        )
        if network == "sandbox"
        else None
    )
    storage_client = StorageNativeNodeClient(
        storage_native_socket,
        telemetry=telemetry,
    )
    storage_status = storage_client.wait_ready()
    memory_backing = None
    if split_memory_backing:
        if (
            storage_status["metrics"].get("hard_capacity_bytes")
            != memory_backing_hard_capacity_bytes
        ):
            raise ValueError("workspace and memory must share one physical disk budget")
        capacity = os.statvfs(volume_mount_root)
        if capacity.f_blocks * capacity.f_frsize < memory_backing_hard_capacity_bytes:
            raise ValueError(
                "memory filesystem is smaller than the physical disk budget"
            )
        memory_backing = MemoryBackingStore(
            volume_mount_root,
            state_root / "memory-backing.sqlite",
            hard_capacity_bytes=memory_backing_hard_capacity_bytes,
            active_root=application_memory_root,
        )
    checkpoint_store = (
        RegistryCheckpointStore(
            RegistryClient(checkpoint_registry_url),
            repository=checkpoint_registry_repository,
        )
        if checkpoint_registry_url
        else None
    )
    registry = DirectSandboxRegistry(
        state_root / "direct-registry.sqlite",
        hard_disk_capacity_mb=memory_backing_hard_capacity_bytes // (1024 * 1024)
        if split_memory_backing else 0,
    )
    if not reflink_memory_restore and registry.reflink_overlap_bytes():
        # A crash may leave a global reservation before the allocator writes
        # its retention row. Keep the cleanup-capable reader until it drains.
        raise ValueError("reflink restore reader is required until overlap capacity drains")
    warden = DirectRunscWarden(
        DirectRunscWardenConfig(
            runsc=runsc,
            runtime_root=state_root / "runsc",
            memory_root=volume_mount_root,
            application_memory_root=application_memory_root,
            reflink_memory_restore=reflink_memory_restore,
            bundle_root=state_root / "bundles",
            journal_root=state_root / "journals",
            runtime_fingerprint=fingerprint,
            network=network,
        ),
        storage=storage_client,
        memory_backing=memory_backing,
        memory_capacity=registry if reflink_memory_restore else None,
        rootfs_lifecycle=overlays,
        telemetry=telemetry,
    )
    provisioner = DirectSandboxProvisioner(
        registry=registry,
        overlays=overlays,
        oci=DirectOciConfigBuilder(
            init_binary=init_binary,
            managed_init_binary=managed_init_binary,
            network_mode=network,
        ),
        warden=warden,
        network_manager=network_manager,
        checkpoint_store=checkpoint_store,
    )
    return DirectSandboxService(
        provisioner,
        max_concurrent_restores=max_concurrent_restores,
        max_concurrent_startups=max_concurrent_startups,
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
