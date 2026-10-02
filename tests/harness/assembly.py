"""Assemble one direct node agent, in the test process or in its own.

``FleetNode`` uses this in process; ``node_process.py`` uses it in a child
process the test can SIGKILL. It imports no harness module, so the child
loads only node-side code. The caller supplies the fakes that differ:
the image store and, where ``os.pidfd_open`` is missing, the fencer.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path

from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.direct_provisioner import DirectSandboxProvisioner
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.direct_warden import (
    DirectRunscWarden,
    DirectRunscWardenConfig,
    SubprocessCommandRunner,
)
from ucloud_sandboxes.disk_claims import DiskClaimPolicy
from ucloud_sandboxes.hibernation import HibernationRuntimeFingerprint
from ucloud_sandboxes.image_rootfs import OverlayRootfsManager
from ucloud_sandboxes.images import DockerImageRuntime
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.heartbeat_sender import HeartbeatSenderConfig
from ucloud_sandboxes.node_agent import build_direct_node_agent_server
from ucloud_sandboxes.runtime_metrics import SingleFlightRuntimeMetricsSampler
from ucloud_sandboxes.storage_native_daemon import StorageNativeNodeClient

HARNESS = Path(__file__).resolve().parent


def _canonical_sha256(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("ascii")
    ).hexdigest()


# One fleet-wide fingerprint, like one pinned runsc on every node: nodes stay
# migration-compatible although each has its own wrapper file.
FINGERPRINT = HibernationRuntimeFingerprint(
    runsc_sha256=hashlib.sha256((HARNESS / "fake_runsc.py").read_bytes()).hexdigest(),
    runsc_commit="f" * 40,
    platform="systrap",
    architecture=os.uname().machine,
    page_size=os.sysconf("SC_PAGE_SIZE"),
    cpu_features_sha256=hashlib.sha256(b"fake-runsc-cpu").hexdigest(),
    boot_config_sha256=_canonical_sha256({
        "network": "none",
        "platform": "systrap",
        "rootfs_format": "ucloud-overlay2-rootfs-v1",
        "quota_layout": "storage-native-v1",
        "runtime": "fake-runsc-v1",
    }),
    rootfs_sha256="0" * 64,
)


def fixed_metrics() -> NodeRuntimeMetrics:
    return NodeRuntimeMetrics(
        collected_at=utc_now(),
        cpu_percent=0.0,
        cpu_count=8,
        load_average_1m=0.0,
        memory_total_mb=65536,
        memory_available_mb=60000,
    )


@dataclass(frozen=True)
class NodeAgentConfig:
    """Everything a node agent needs; JSON-encodable for the child process."""

    bin: str
    proc_root: str
    state_root: str
    volumes: str
    runtime_root: str
    storage_socket: str
    init_binary: str
    job_id: str
    node_id: str
    deployment_id: str
    node_control_token: str
    node_epoch: str
    port: int
    url: str | None
    admission_wait_seconds: float
    heartbeat_url: str
    heartbeat_token: str
    # Scenarios send heartbeats explicitly; the periodic one never fires in a test.
    heartbeat_interval_seconds: float = 3600.0


def assemble_node_agent(config: NodeAgentConfig, *, rootfs_store, fencer, sample_metrics):
    """Return ``(server, service)``, assembled as ``cmd_serve_direct_node_agent`` does."""
    bin_dir, state_root = Path(config.bin), Path(config.state_root)
    overlays = OverlayRootfsManager(
        rootfs_store,
        writable_root=Path(config.volumes),
        bundle_root=state_root / "bundles",
        runner=SubprocessCommandRunner(),
        mount_binary=str(bin_dir / "mount"),
        mountpoint_binary=str(bin_dir / "mountpoint"),
        umount_binary=str(bin_dir / "umount"),
        require_precreated_writable=True,
    )
    warden = DirectRunscWarden(
        DirectRunscWardenConfig(
            runsc=bin_dir / "runsc",
            runtime_root=Path(config.runtime_root),
            memory_root=Path(config.volumes),
            bundle_root=state_root / "bundles",
            journal_root=state_root / "journals",
            runtime_fingerprint=FINGERPRINT,
            proc_root=Path(config.proc_root),
            network="none",
            command_timeout_seconds=30.0,
            stop_timeout_seconds=10.0,
        ),
        storage=StorageNativeNodeClient(Path(config.storage_socket)),
        rootfs_lifecycle=overlays,
        fencer=fencer,
    )
    provisioner = DirectSandboxProvisioner(
        registry=DirectSandboxRegistry(state_root / "direct-registry.sqlite", owner=True),
        overlays=overlays,
        oci=DirectOciConfigBuilder(init_binary=Path(config.init_binary), network_mode="none"),
        warden=warden,
        disk_claim_policy=DiskClaimPolicy(),
    )
    service = DirectSandboxService(provisioner, max_concurrent_restores=4, max_concurrent_startups=4)
    service.admission_wait_seconds = config.admission_wait_seconds
    server = build_direct_node_agent_server(
        "127.0.0.1",
        config.port,
        service=service,
        image_file=state_root / "images.json",
        job_id=config.job_id,
        node_id=config.node_id,
        node_url=config.url,
        deployment_id=config.deployment_id,
        total_resources=ResourceQuantity(vcpu=8, memory_mb=16384, disk_mb=65536),
        image_runtime=DockerImageRuntime(dry_run=True),
        node_control_bearer_token=config.node_control_token,
        # Uncached, so a sample the test sets governs the very next admission.
        runtime_metrics_provider=SingleFlightRuntimeMetricsSampler(sample_metrics, freshness_seconds=0),
        node_epoch=config.node_epoch,
        heartbeat=HeartbeatSenderConfig(
            url=config.heartbeat_url,
            bearer_token=config.heartbeat_token,
            interval_seconds=config.heartbeat_interval_seconds,
        ),
    )
    if config.url is None:
        # The URL is part of route identity; restarts rebind this port.
        server.RequestHandlerClass.node_url = f"http://127.0.0.1:{server.server_address[1]}"
    return server, service
