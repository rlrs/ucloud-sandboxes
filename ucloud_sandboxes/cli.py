from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
from threading import Event
import time
from typing import Any, Callable, Iterable
from urllib.error import HTTPError
from urllib.parse import quote, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4

from opentelemetry.propagate import inject

from .agent import (
    build_heartbeat,
    default_node_id,
    detect_job_id,
    fetch_node_agent_heartbeat,
    post_heartbeat,
    post_heartbeat_with_headers,
)
from .autoscaler_state import (
    AutoscalerStateError,
    AutoscalerProcessLock,
    AutoscalerStateStore,
    DrainIntent,
    ProviderOperation,
    ProviderOperationOutcome,
    stable_provider_operation_id,
)
from .capabilities import (
    STORAGE_NATIVE_CAPABILITY,
    STORAGE_NATIVE_DETACH_CAPABILITY,
    merge_capabilities,
)
from .bootstrap import (
    VmBootstrapIntent,
    VmBootstrapRecord,
    build_vm_bootstrap_intents,
    mark_bootstrap_access_refresh,
    mark_bootstrap_attempt,
    mark_bootstrap_failure,
    mark_bootstrap_success,
    prune_bootstrap_records,
)
from .config import DeploymentConfig
from .control_state import ControlStateStore
from .control_plane import build_server
from .deployment import (
    AGENT_VERSION_LABEL,
    BUILDER_LABEL,
    DEFAULT_INIT_VERSION,
    DEPLOYMENT_LABEL,
    GATEWAY_LABEL,
    INIT_VERSION_LABEL,
    NODE_LABEL,
    package_version,
)
from .deploy import (
    AllInOneDeployPlan,
    DEFAULT_INSTALL_ROOT,
    read_remote_text_over_ssh,
    render_remote_deploy_script,
    run_remote_script_over_ssh,
    stage_file_over_ssh,
)
from .images import DockerImageRuntime, ImageRecord, ImageStore
from .managed_registry import (
    RegistryClient,
    RegistryRequestError,
    RegistryUsageGenerationChanged,
    RegistryUsageStore,
    apply_registry_usage,
    execute_registry_prune,
    list_registry_tags,
    registry_host_from_image_ref,
    registry_repository_tag_from_image_ref,
    registry_prune_plan,
    select_prune_candidates,
)
from .metrics import (
    MetricsStore,
    build_live_scale_signals,
    record_autoscaler_cycle,
    record_vm_init_attempt,
    record_vm_observed,
    record_vm_submitted,
)
from .model_relay import (
    DEFAULT_MAX_COMPLETED_BYTES,
    DEFAULT_MAX_INFLIGHT_BYTES,
    DEFAULT_MAX_INFLIGHT_REQUESTS,
    DEFAULT_MAX_INFLIGHT_REQUESTS_PER_ROLLOUT,
    RelayRequest,
    create_model_relay_app,
)
from .models import (
    NodeHeartbeat,
    ResourceQuantity,
    SandboxDemand,
    SandboxNode,
    SandboxPlacementRequest,
    ScalePolicy,
    ProviderInstance,
    utc_now,
)
from .providers.base import (
    ComputeProvider,
    InstanceBootstrapAccess,
    InstanceCreateIntent,
    ProviderError,
)
from .providers.loader import load_external_provider
from .providers.ucloud import (
    UCloudSettings,
    bootstrap_access_from_payload,
    instance_from_payload,
)
from .providers.ucloud.composition import (
    provider_from_configuration as ucloud_provider_from_configuration,
)
from .networking import (
    stable_hostname,
)
from .policy import (
    evaluate_scale,
    unreachable_node_reference,
    unreachable_node_stop_ready,
)
from .program_scheduler import (
    WakeNodeCandidate,
    build_program_scale_signals,
    node_pressure_score,
    plan_shadow_wake_queue,
)
from .reconcile import (
    build_create_intents,
    evaluate_builder_scale,
    node_drain_ready,
    partition_safe_stop_job_ids,
    with_provider_operation_label,
)
from .registry import (
    heartbeat_to_dict,
    merge_jobs_and_heartbeats,
)
from .routing import (
    ProgramRequestState,
    RoutingStore,
    SandboxRoute,
    is_portable_parked_route,
    is_worker_detachable_parked_route,
    sandbox_demand_from_routing_state,
)
from .telemetry import Telemetry, TelemetrySettings
from .providers.ucloud.api import (
    SessionStore,
    UCloudClient,
    UCloudError,
)
from .vm_init import (
    DEFAULT_MAX_CONCURRENT_IMAGE_PULLS,
    DEFAULT_MANAGED_INIT,
    DEFAULT_STORAGE_NATIVE_MAX_UBLK_DEVICES,
    DEFAULT_UCLOUD_NODE_STATE_DIR,
    DEFAULT_UCLOUD_STORAGE_NATIVE_MAX_UBLK_DEVICES,
    VmInitOptions,
    render_vm_init_script,
    run_init_over_ssh,
    stage_vm_init_package_over_ssh,
)
from .providers.ucloud.payloads import (
    DEFAULT_PUBLIC_LINK_PORT,
    DEFAULT_GATEWAY_VM_PRODUCT_ID,
    DEFAULT_VM_APPLICATION_NAME,
    DEFAULT_VM_APPLICATION_VERSION,
    DEFAULT_VM_DISK_GB,
    DEFAULT_VM_PRODUCT_CATEGORY,
    DEFAULT_VM_PRODUCT_ID,
    DEFAULT_VM_PRODUCT_PROVIDER,
    VmApplicationRef,
    VmFileMount,
    VmProductRef,
    VmSubmissionOptions,
    VmTimeAllocation,
    PrivateNetworkAttachment,
    PublicLinkAttachment,
    apply_private_network_attachment,
    apply_public_link_attachment,
)


_MAX_CONTROL_RESPONSE_BYTES = 1024 * 1024
_DISABLED_AUTOSCALER_TELEMETRY = Telemetry.disabled("autoscaler")


def ucloud_settings(
    config: DeploymentConfig,
    session_file: Path | None = None,
) -> UCloudSettings:
    settings = UCloudSettings.from_provider(config.provider)
    configured_session = (
        settings.session_file
        if "session_file" in config.provider.settings
        else str(config.session_file())
    )
    return replace(
        settings,
        session_file=str(session_file or configured_session),
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (
        OSError,
        ValueError,
        ProviderError,
        UCloudError,
        AutoscalerStateError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ucloud-sandboxes",
        description=(
            "Autoscale direct gVisor sandbox nodes through a compute provider."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {package_version()}",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sample = subparsers.add_parser("sample-config", help="Print a sample JSON config.")
    sample.set_defaults(func=cmd_sample_config)

    inspect_job = subparsers.add_parser(
        "inspect-job", help="Inspect one UCloud VM job."
    )
    add_config_args(inspect_job)
    add_session_arg(inspect_job)
    inspect_job.add_argument("job_id")
    inspect_job.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    inspect_job.set_defaults(func=cmd_inspect_job)

    agent_heartbeat = subparsers.add_parser(
        "agent-heartbeat",
        help="Emit or submit one VM node heartbeat.",
    )
    agent_heartbeat.add_argument("--deployment-id", required=True)
    agent_heartbeat.add_argument("--job-id", help="UCloud VM job id.")
    agent_heartbeat.add_argument(
        "--node-id", help="Stable node id. Defaults to hostname."
    )
    agent_heartbeat.add_argument(
        "--node-url",
        help="URL the control plane can use to reach this node agent.",
    )
    agent_heartbeat.add_argument(
        "--active",
        type=int,
        default=0,
        help="Currently active sandboxes on this node.",
    )
    agent_heartbeat.add_argument(
        "--draining",
        action="store_true",
        help="Mark node as draining.",
    )
    agent_heartbeat.add_argument(
        "--capability",
        action="append",
        default=[],
        help="Advertise a node capability, e.g. sandbox or image-build.",
    )
    add_node_version_args(agent_heartbeat)
    add_resource_args(agent_heartbeat)
    agent_heartbeat.add_argument(
        "--label",
        action="append",
        default=[],
        help="Heartbeat label as key=value. Repeat for multiple labels.",
    )
    agent_heartbeat.add_argument(
        "--post-url",
        help="Control-plane heartbeat URL, e.g. http://127.0.0.1:8080/v1/nodes/heartbeat.",
    )
    agent_heartbeat.add_argument(
        "--bearer-token-file",
        type=Path,
        help="Read a bearer token from this file when posting the heartbeat.",
    )
    agent_heartbeat.add_argument(
        "--from-node-agent-url",
        help=(
            "Fetch the live heartbeat from a running node-agent /v1/heartbeat "
            "instead of building a static heartbeat from CLI flags."
        ),
    )
    agent_heartbeat.add_argument(
        "--node-control-bearer-token-file",
        type=Path,
        help="Authenticate the local node-agent heartbeat fetch with this token.",
    )
    agent_heartbeat.add_argument(
        "--control-state-file",
        type=Path,
        help="Local control-state database to upsert into when explicitly supplied.",
    )
    agent_heartbeat.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    agent_heartbeat.set_defaults(func=cmd_agent_heartbeat)

    serve = subparsers.add_parser(
        "serve-control-plane",
        help="Run the gateway/control-plane service.",
    )
    add_config_args(serve)
    serve.add_argument("--host", default="0.0.0.0", help="Bind host.")
    serve.set_defaults(func=cmd_serve_control_plane)

    direct_node_agent = subparsers.add_parser(
        "serve-direct-node-agent",
        help="Run the direct-runsc sandbox node daemon.",
    )
    direct_node_agent.add_argument("--deployment-id", required=True)
    direct_node_agent.add_argument("--host", default="127.0.0.1")
    direct_node_agent.add_argument("--port", type=int, default=8090)
    direct_node_agent.add_argument("--job-id")
    direct_node_agent.add_argument("--node-id")
    direct_node_agent.add_argument("--node-url")
    add_node_version_args(direct_node_agent)
    add_resource_args(direct_node_agent)
    direct_node_agent.add_argument("--state-root", type=Path, required=True)
    direct_node_agent.add_argument(
        "--image-cache-root",
        type=Path,
        help=(
            "Node-local directory for disposable materialized image rootfses. "
            "Defaults to <state-root>/image-cache."
        ),
    )
    direct_node_agent.add_argument("--image-file", type=Path, required=True)
    direct_node_agent.add_argument("--volume-mount-root", type=Path, required=True)
    direct_node_agent.add_argument(
        "--storage-native-socket",
        type=Path,
        required=True,
        help="Root-only storage-native node service socket.",
    )
    direct_node_agent.add_argument("--runsc", type=Path, required=True)
    direct_node_agent.add_argument("--runsc-commit", required=True)
    direct_node_agent.add_argument(
        "--init-binary",
        type=Path,
        default=Path("/usr/libexec/docker-init"),
    )
    direct_node_agent.add_argument(
        "--managed-init-binary",
        type=Path,
        default=Path(DEFAULT_MANAGED_INIT),
    )
    direct_node_agent.add_argument("--docker-binary", default="docker")
    direct_node_agent.add_argument(
        "--max-concurrent-image-pulls",
        type=int,
        default=DEFAULT_MAX_CONCURRENT_IMAGE_PULLS,
        help="Maximum distinct cold Docker pulls running concurrently on this node.",
    )
    direct_node_agent.add_argument(
        "--max-concurrent-restores",
        type=int,
        default=8,
    )
    direct_node_agent.add_argument(
        "--idle-park-seconds",
        type=float,
        default=0.0,
        help=(
            "Optional host-API inactivity heuristic. Disabled by default because "
            "it cannot observe work executing inside the sandbox; production "
            "parking should be requested explicitly by the relay or client."
        ),
    )
    direct_node_agent.add_argument(
        "--network",
        choices=("none", "sandbox"),
        default="none",
        help=(
            "Node-wide gVisor network mode. sandbox uses node-owned isolated "
            "network namespaces, veth links, and NAT."
        ),
    )
    direct_node_agent.add_argument(
        "--direct-network-allow-tcp",
        action="append",
        default=[],
        metavar="IPV4:PORT",
        help=(
            "Allow sandbox egress to one exact private TCP service before the "
            "RFC1918 deny rules. Repeat for multiple infrastructure services."
        ),
    )
    direct_node_agent.add_argument(
        "--node-control-bearer-token-file",
        type=Path,
        required=True,
    )
    add_telemetry_args(direct_node_agent)
    direct_node_agent.set_defaults(func=cmd_serve_direct_node_agent)

    builder_agent = subparsers.add_parser(
        "serve-builder-agent",
        help="Run an image-only builder node.",
    )
    builder_agent.add_argument("--deployment-id", required=True)
    builder_agent.add_argument("--host", default="127.0.0.1")
    builder_agent.add_argument("--port", type=int, default=8090)
    builder_agent.add_argument("--job-id")
    builder_agent.add_argument("--node-id")
    builder_agent.add_argument("--node-url")
    add_node_version_args(builder_agent)
    add_resource_args(builder_agent)
    builder_agent.add_argument("--state-file", type=Path, required=True)
    builder_agent.add_argument("--image-file", type=Path, required=True)
    builder_agent.add_argument("--docker-binary", default="docker")
    builder_agent.add_argument("--buildx-direct-push", action="store_true")
    builder_agent.add_argument("--buildx-cache-ref")
    builder_agent.add_argument("--max-active-image-builds", type=int, default=4)
    builder_agent.add_argument(
        "--max-concurrent-image-pulls",
        type=int,
        default=DEFAULT_MAX_CONCURRENT_IMAGE_PULLS,
    )
    builder_agent.add_argument(
        "--node-control-bearer-token-file", type=Path, required=True
    )
    add_telemetry_args(builder_agent)
    builder_agent.set_defaults(func=cmd_serve_builder_agent)

    model_relay = subparsers.add_parser(
        "serve-model-relay",
        help="Run the deployment model relay.",
    )
    add_config_args(model_relay)
    model_relay.add_argument("--host", default="0.0.0.0", help="Bind host.")
    model_relay.set_defaults(func=cmd_serve_model_relay)

    init_vm = subparsers.add_parser(
        "init-vm",
        help="Plan or execute strict deployment init for one running VM.",
    )
    add_config_args(init_vm)
    add_session_arg(init_vm)
    init_vm.add_argument("job_id", help="UCloud VM job id.")
    init_vm.add_argument("--role", choices=("sandbox", "builder"), required=True)
    init_vm.add_argument("--package-spec", type=Path, required=True)
    init_vm.add_argument("--execute", action="store_true")
    init_vm.add_argument("--timeout-seconds", type=int, default=1800)
    init_vm.add_argument("--ssh-private-key-file")
    init_vm.add_argument("--output", choices=("text", "json"), default="text")
    init_vm.set_defaults(func=cmd_init_vm)

    ensure_ssh_key = subparsers.add_parser(
        "ensure-ucloud-ssh-key",
        help="Create a UCloud account SSH key if the public key is not already registered.",
    )
    add_config_args(ensure_ssh_key)
    add_session_arg(ensure_ssh_key)
    ensure_ssh_key.add_argument(
        "--title",
        default="ucloud-sandboxes gateway init",
        help="Title used when creating the UCloud SSH key.",
    )
    ensure_ssh_key.add_argument(
        "--public-key-file",
        required=True,
        type=Path,
        help="OpenSSH public key file to register with UCloud.",
    )
    ensure_ssh_key.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    ensure_ssh_key.set_defaults(func=cmd_ensure_ucloud_ssh_key)

    network_attachment = subparsers.add_parser(
        "vm-network-attachment",
        help="Render the UCloud job fragment for private-network VM membership.",
    )
    add_config_args(network_attachment)
    network_attachment.add_argument(
        "--private-network-id",
        help="UCloud private network resource id.",
    )
    network_attachment.add_argument(
        "--hostname",
        help="Hostname used by this VM inside the private network.",
    )
    network_attachment.add_argument(
        "--hostname-seed",
        help="Seed for generating a stable hostname when --hostname is omitted.",
    )
    network_attachment.add_argument(
        "--hostname-prefix",
        help="Prefix used with --hostname-seed. Defaults to sandbox-node.",
    )
    network_attachment.add_argument(
        "--output", choices=("text", "json"), default="json", help="Output format."
    )
    network_attachment.set_defaults(func=cmd_vm_network_attachment)

    public_link_attachment = subparsers.add_parser(
        "vm-public-link-attachment",
        help="Render the UCloud job fragment for binding a public link to a VM port.",
    )
    add_config_args(public_link_attachment)
    public_link_attachment.add_argument(
        "--public-link-id",
        help="UCloud public link resource id. Defaults to provider.gateway_public_link_id.",
    )
    public_link_attachment.add_argument(
        "--port",
        type=int,
        help=(
            "VM-local port exposed through the public link. Defaults to "
            f"provider.gateway_public_link_port or {DEFAULT_PUBLIC_LINK_PORT}."
        ),
    )
    public_link_attachment.add_argument(
        "--output", choices=("text", "json"), default="json", help="Output format."
    )
    public_link_attachment.set_defaults(func=cmd_vm_public_link_attachment)

    registry_prune = subparsers.add_parser(
        "registry-prune",
        help="Plan or execute deployment registry retention.",
    )
    add_config_args(registry_prune)
    registry_prune.add_argument("--repository-prefix", default="")
    registry_prune.add_argument("--execute", action="store_true")
    registry_prune.set_defaults(func=cmd_registry_prune)

    submit_vm = subparsers.add_parser(
        "submit-vm",
        help="Render or submit one UCloud VM job, including gateway VMs.",
    )
    add_config_args(submit_vm)
    add_session_arg(submit_vm)
    submit_vm.add_argument("--name", help="UCloud job name.")
    submit_vm.add_argument(
        "--role",
        choices=("node", "gateway", "builder"),
        default="node",
        help=(
            "VM role. Gateway and builder VMs are not labelled as autoscaled "
            "sandbox nodes."
        ),
    )
    submit_vm.add_argument(
        "--hostname",
        help="Hostname used by this VM inside the private network.",
    )
    submit_vm.add_argument(
        "--hostname-seed",
        help="Seed for job name and hostname generation. Defaults to a random suffix.",
    )
    submit_vm.add_argument(
        "--hostname-prefix",
        help="Hostname prefix. Defaults to the selected role.",
    )
    submit_vm.add_argument(
        "--private-network-id",
        help="UCloud private network id. Defaults to provider.private_network_id.",
    )
    submit_vm.add_argument(
        "--no-private-network",
        action="store_true",
        help="Submit without private-network attachment.",
    )
    submit_vm.add_argument(
        "--public-link-id",
        help="UCloud public link resource id to bind to this VM.",
    )
    submit_vm.add_argument(
        "--public-link-port",
        type=int,
        help=(
            "VM-local port exposed through --public-link-id. Defaults to "
            f"provider.gateway_public_link_port or {DEFAULT_PUBLIC_LINK_PORT}."
        ),
    )
    submit_vm.add_argument(
        "--no-public-link",
        action="store_true",
        help="Submit without public-link attachment even if config has one.",
    )
    submit_vm.add_argument(
        "--mount",
        action="append",
        default=[],
        help=(
            "Attach a read-write UCloud project file/folder path. The VM app "
            "mounts it under /work/<name>. Repeat for multiple mounts."
        ),
    )
    submit_vm.add_argument(
        "--mount-ro",
        action="append",
        default=[],
        help=(
            "Attach a read-only UCloud project file/folder path. The VM app "
            "mounts it under /work/<name>. Repeat for multiple mounts."
        ),
    )
    submit_vm.add_argument(
        "--app-name",
        default=DEFAULT_VM_APPLICATION_NAME,
        help="UCloud VM application name.",
    )
    submit_vm.add_argument(
        "--app-version",
        default=DEFAULT_VM_APPLICATION_VERSION,
        help="UCloud VM application version.",
    )
    submit_vm.add_argument(
        "--product-id",
        default=None,
        help=(
            "UCloud VM product id. Defaults to "
            f"{DEFAULT_GATEWAY_VM_PRODUCT_ID} for --role gateway and "
            f"{DEFAULT_VM_PRODUCT_ID} otherwise."
        ),
    )
    submit_vm.add_argument(
        "--product-category",
        default=DEFAULT_VM_PRODUCT_CATEGORY,
        help="UCloud VM product category.",
    )
    submit_vm.add_argument(
        "--product-provider",
        default=DEFAULT_VM_PRODUCT_PROVIDER,
        help="UCloud VM product provider.",
    )
    submit_vm.add_argument(
        "--disk-gb",
        type=int,
        default=DEFAULT_VM_DISK_GB,
        help="VM disk size parameter in GB.",
    )
    submit_vm.add_argument(
        "--time-hours",
        type=int,
        default=1,
        help="VM time allocation hours.",
    )
    submit_vm.add_argument(
        "--time-minutes",
        type=int,
        default=0,
        help="VM time allocation minutes.",
    )
    submit_vm.add_argument(
        "--time-seconds",
        type=int,
        default=0,
        help="VM time allocation seconds.",
    )
    submit_vm.add_argument(
        "--ssh",
        action="store_true",
        help=(
            "Request sshEnabled=true. The current vm-ubuntu:24.04 app rejects this "
            "on the live API."
        ),
    )
    submit_vm.add_argument(
        "--no-ssh",
        action="store_true",
        help="Submit without sshEnabled=true. This is the default.",
    )
    submit_vm.add_argument(
        "--allow-duplicate-job",
        action="store_true",
        help="Allow UCloud to submit even when it detects a duplicate job.",
    )
    submit_vm.add_argument(
        "--label",
        action="append",
        default=[],
        help="UCloud job label as key=value. Repeat for multiple labels.",
    )
    submit_vm.add_argument(
        "--execute",
        action="store_true",
        help="Actually submit the VM job. Default is dry-run.",
    )
    submit_vm.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    submit_vm.set_defaults(func=cmd_submit_vm)

    open_vm_web = subparsers.add_parser(
        "open-vm-web",
        help="Open/configure a UCloud VM web session for a public-link target port.",
    )
    add_config_args(open_vm_web)
    add_session_arg(open_vm_web)
    open_vm_web.add_argument("job_id", help="UCloud VM job id.")
    open_vm_web.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PUBLIC_LINK_PORT,
        help=f"VM-local web target port. Defaults to {DEFAULT_PUBLIC_LINK_PORT}.",
    )
    open_vm_web.add_argument(
        "--rank",
        type=int,
        default=0,
        help="Replica rank for the VM job. Defaults to 0.",
    )
    open_vm_web.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    open_vm_web.set_defaults(func=cmd_open_vm_web)

    deploy_all = subparsers.add_parser(
        "deploy-all-in-one",
        help="Install one strict deployment on a running gateway VM.",
    )
    add_config_args(deploy_all)
    add_session_arg(deploy_all)
    deploy_all.add_argument("job_id", help="Running gateway VM job id.")
    deploy_all.add_argument("--wheel", required=True, type=Path)
    deploy_all.add_argument("--direct-runsc", required=True, type=Path)
    deploy_all.add_argument("--managed-init", required=True, type=Path)
    deploy_all.add_argument("--storage-native-manifest", required=True, type=Path)
    deploy_all.add_argument("--ssh-command")
    deploy_all.add_argument("--ssh-private-key-file")
    deploy_all.add_argument("--ssh-key-title")
    deploy_all.add_argument("--no-copy-session", action="store_true")
    deploy_all.add_argument("--no-open-public-links", action="store_true")
    deploy_all.add_argument("--timeout-seconds", type=int, default=1800)
    deploy_all.add_argument("--execute", action="store_true")
    deploy_all.add_argument(
        "--output", choices=("text", "json", "script"), default="text"
    )
    deploy_all.set_defaults(func=cmd_deploy_all_in_one)

    heartbeats = subparsers.add_parser(
        "heartbeats",
        help="List stored node heartbeats.",
    )
    add_config_args(heartbeats)
    heartbeats.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    heartbeats.set_defaults(func=cmd_heartbeats)

    loop = subparsers.add_parser(
        "autoscaler",
        help="Plan or execute the deployment autoscaler.",
    )
    add_config_args(loop)
    loop.add_argument("--once", action="store_true")
    loop.add_argument("--jobs-file", type=Path)
    loop.add_argument("--include-job", action="append", default=[])
    loop.add_argument("--seed-prefix")
    loop.add_argument("--execute", action="store_true")
    loop.add_argument("--output", choices=("text", "json"), default="text")
    loop.set_defaults(func=cmd_autoscaler)
    return parser


def add_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Exact deployment JSON configuration.",
    )


def add_session_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--session-file",
        type=Path,
        help="Operational local UCloud credential override.",
    )


def add_node_version_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--agent-version",
        default=package_version(),
        help="Node agent version advertised in heartbeats.",
    )
    parser.add_argument(
        "--init-version",
        default=DEFAULT_INIT_VERSION,
        help="VM init script contract version advertised in heartbeats.",
    )


def add_resource_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--total-vcpu", type=float, default=0.0, help="Node physical vCPU."
    )
    parser.add_argument(
        "--total-memory-mb", type=int, default=0, help="Node physical RAM in MB."
    )
    parser.add_argument(
        "--total-disk-mb", type=int, default=0, help="Node usable sandbox disk in MB."
    )


def add_telemetry_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--telemetry-otlp-endpoint", default="")
    parser.add_argument("--telemetry-cloud-provider", default="")
    parser.add_argument("--telemetry-cloud-machine-type", default="")
    parser.add_argument("--telemetry-trace-sample-ratio", type=float, default=0.1)
    parser.add_argument("--telemetry-export-interval-ms", type=int, default=5_000)
    parser.add_argument("--telemetry-export-timeout-ms", type=int, default=3_000)
    parser.add_argument("--telemetry-max-queue-size", type=int, default=4_096)
    parser.add_argument("--telemetry-max-export-batch-size", type=int, default=512)


def load_config(args: argparse.Namespace) -> DeploymentConfig:
    config = DeploymentConfig.from_file(args.config)
    session_file = getattr(args, "session_file", None)
    if session_file is None:
        return config
    return replace(
        config,
        provider=config.provider.with_setting("session_file", str(session_file)),
    )


def telemetry_from_config(
    config: DeploymentConfig,
    service_name: str,
    *,
    attributes: dict[str, str | int | float | bool] | None = None,
) -> Telemetry:
    resource_attributes: dict[str, str | int | float | bool] = {
        "cloud.provider": config.provider.kind,
        **dict(attributes or {}),
    }
    return Telemetry.create(
        config.telemetry,
        service_name=service_name,
        service_version=package_version(),
        deployment_id=config.deployment_id,
        attributes=resource_attributes,
    )


def telemetry_from_args(
    args: argparse.Namespace,
    service_name: str,
    *,
    deployment_id: str,
    attributes: dict[str, str | int | float | bool] | None = None,
) -> Telemetry:
    resource_attributes: dict[str, str | int | float | bool] = {
        **dict(attributes or {}),
    }
    if args.telemetry_cloud_provider:
        resource_attributes["cloud.provider"] = args.telemetry_cloud_provider
    if args.telemetry_cloud_machine_type:
        resource_attributes["cloud.machine.type"] = args.telemetry_cloud_machine_type
    return Telemetry.create(
        TelemetrySettings(
            endpoint=args.telemetry_otlp_endpoint,
            trace_sample_ratio=args.telemetry_trace_sample_ratio,
            export_interval_ms=args.telemetry_export_interval_ms,
            export_timeout_ms=args.telemetry_export_timeout_ms,
            max_queue_size=args.telemetry_max_queue_size,
            max_export_batch_size=args.telemetry_max_export_batch_size,
        ),
        service_name=service_name,
        service_version=package_version(),
        deployment_id=deployment_id,
        attributes=resource_attributes,
    )


def cmd_sample_config(_args: argparse.Namespace) -> int:
    print(json.dumps(DeploymentConfig.default().to_dict(), indent=2, sort_keys=True))
    return 0


def cmd_inspect_job(args: argparse.Namespace) -> int:
    config = load_config(args)
    if not ucloud_settings(config).project_id:
        raise ValueError("provider.scope_id is required")
    client = UCloudClient(SessionStore(Path(ucloud_settings(config).session_file)))
    payload = client.retrieve_job(ucloud_settings(config).project_id, args.job_id)
    job = instance_from_payload(payload)
    if args.output == "json":
        print_json(vm_job_to_dict(job))
    else:
        print_vm_job(job)
    return 0


def cmd_agent_heartbeat(args: argparse.Namespace) -> int:
    labels = parse_labels(getattr(args, "label", []))
    if args.from_node_agent_url:
        node_control_token = read_required_token_file(
            getattr(args, "node_control_bearer_token_file", None),
            "node control bearer token",
        )
        heartbeat = fetch_node_agent_heartbeat(
            args.from_node_agent_url,
            bearer_token=node_control_token,
        )
        if labels:
            heartbeat = replace(heartbeat, labels={**heartbeat.labels, **labels})
    else:
        job_id = args.job_id or detect_job_id()
        if not job_id:
            raise ValueError("job id is required via --job-id or UCLOUD_JOB_ID.")
        heartbeat = build_heartbeat(
            job_id=job_id,
            node_id=args.node_id,
            active_sandboxes=args.active,
            draining=args.draining,
            node_url=args.node_url,
            agent_version=args.agent_version,
            deployment_id=args.deployment_id,
            init_version=args.init_version,
            capabilities=merge_capabilities(tuple(args.capability)),
            total_resources=resource_quantity_from_args(args),
            used_resources=ResourceQuantity(),
            labels=labels,
        )

    result: dict[str, Any] = {"heartbeat": heartbeat_to_dict(heartbeat)}
    if args.control_state_file:
        store = ControlStateStore(args.control_state_file)
        store.upsert_heartbeat(heartbeat)
        result["controlStateFile"] = str(args.control_state_file)
    if args.post_url:
        if args.bearer_token_file:
            token = args.bearer_token_file.read_text(encoding="utf-8").strip()
            if not token:
                raise ValueError("bearer token file is empty.")
            post_result = post_heartbeat_with_headers(
                args.post_url,
                heartbeat,
                {"Authorization": f"Bearer {token}"},
            )
        else:
            post_result = post_heartbeat(args.post_url, heartbeat)
        result["post"] = {
            "status": post_result.status,
            "payload": post_result.payload,
        }
        if post_result.status >= 400:
            raise ValueError(f"heartbeat POST failed with HTTP {post_result.status}")

    if args.output == "json":
        print_json(result)
    else:
        print(f"Heartbeat: node={heartbeat.node_id} job={heartbeat.job_id}")
        if heartbeat.node_url:
            print(f"Node URL: {heartbeat.node_url}")
        print(f"Active: {heartbeat.active_sandboxes}, draining: {heartbeat.draining}")
        if args.control_state_file:
            print(f"Wrote: {args.control_state_file}")
        if args.post_url:
            print(f"Posted: {args.post_url}")
        if not args.control_state_file and not args.post_url:
            print_json(heartbeat_to_dict(heartbeat))
    return 0


def cmd_serve_control_plane(args: argparse.Namespace) -> int:
    config = load_config(args)
    telemetry = telemetry_from_config(config, "ucloud-sandboxes-gateway")
    server = build_server(
        args.host,
        config.gateway_port,
        config.control_state_file(),
        routing_file=config.routing_file(),
        gateway_bearer_token=read_required_token_file(
            config.gateway_token_file(), "gateway bearer token"
        ),
        sandbox_api_token=read_required_token_file(
            config.sandbox_api_token_file(), "sandbox API token"
        ),
        heartbeat_bearer_token=read_required_token_file(
            config.heartbeat_token_file(), "heartbeat bearer token"
        ),
        node_control_bearer_token=read_required_token_file(
            config.node_control_token_file(), "node control bearer token"
        ),
        deployment_id=config.deployment_id,
        heartbeat_ttl_seconds=config.gateway_heartbeat_ttl_seconds,
        image_file=config.image_file(),
        metrics_file=config.metrics_path(),
        registry_url=config.registry_url,
        registry_worker_url=config.registry_worker_url,
        registry_usage_file=config.registry_usage_file(),
        max_concurrent_sandbox_creates=(config.gateway_max_concurrent_sandbox_creates),
        max_http_request_threads=config.gateway_max_http_request_threads,
        max_sandbox_resources=config.sandbox.resources,
        telemetry=telemetry,
    )
    host, port = server.server_address
    print(f"Serving gateway on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping gateway.")
    finally:
        server.server_close()
        telemetry.shutdown()
    return 0


def cmd_serve_builder_agent(args: argparse.Namespace) -> int:
    from .node_agent import build_builder_node_agent_server

    job_id = args.job_id or detect_job_id()
    if not job_id:
        raise ValueError("job id is required via --job-id or UCLOUD_JOB_ID.")
    node_id = args.node_id or default_node_id(job_id)
    telemetry = telemetry_from_args(
        args,
        "ucloud-sandboxes-builder",
        deployment_id=args.deployment_id,
        attributes={"service.instance.id": node_id, "cloud.instance.id": job_id},
    )
    server = build_builder_node_agent_server(
        args.host,
        args.port,
        state_file=args.state_file.absolute(),
        image_file=args.image_file.absolute(),
        job_id=job_id,
        node_id=node_id,
        node_url=args.node_url,
        agent_version=args.agent_version,
        deployment_id=args.deployment_id,
        init_version=args.init_version,
        total_resources=resource_quantity_from_args(args),
        image_runtime=DockerImageRuntime(
            docker_binary=args.docker_binary,
            dry_run=False,
            buildx_direct_push=args.buildx_direct_push,
            buildx_cache_ref=args.buildx_cache_ref,
        ),
        max_active_image_builds=args.max_active_image_builds,
        max_concurrent_image_pulls=args.max_concurrent_image_pulls,
        node_control_bearer_token=read_required_token_file(
            args.node_control_bearer_token_file,
            "node control bearer token",
        ),
        telemetry=telemetry,
    )
    host, port = server.server_address
    print(f"Serving builder node agent on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping builder node agent.")
    finally:
        server.server_close()
        telemetry.shutdown()
    return 0


def cmd_serve_direct_node_agent(args: argparse.Namespace) -> int:
    from .direct_runtime import build_direct_runtime_service
    from .node_agent import build_direct_node_agent_server

    job_id = args.job_id or detect_job_id()
    if not job_id:
        raise ValueError("job id is required via --job-id or UCLOUD_JOB_ID.")
    node_id = args.node_id or default_node_id(job_id)
    telemetry = telemetry_from_args(
        args,
        "ucloud-sandboxes-worker",
        deployment_id=args.deployment_id,
        attributes={"service.instance.id": node_id, "cloud.instance.id": job_id},
    )
    state_root = args.state_root
    image_file = args.image_file
    service = build_direct_runtime_service(
        state_root=state_root.absolute(),
        image_cache_root=(
            args.image_cache_root.absolute()
            if args.image_cache_root is not None
            else None
        ),
        volume_mount_root=args.volume_mount_root.absolute(),
        runsc=args.runsc.absolute(),
        runsc_commit=args.runsc_commit,
        init_binary=args.init_binary.absolute(),
        managed_init_binary=args.managed_init_binary.absolute(),
        docker_binary=args.docker_binary,
        network=args.network,
        network_allow_tcp=tuple(args.direct_network_allow_tcp or ()),
        max_concurrent_restores=args.max_concurrent_restores,
        idle_park_seconds=float(args.idle_park_seconds),
        storage_native_socket=args.storage_native_socket.absolute(),
        telemetry=telemetry,
    )
    server = build_direct_node_agent_server(
        args.host,
        args.port,
        service=service,
        image_file=image_file,
        job_id=job_id,
        node_id=node_id,
        node_url=args.node_url,
        agent_version=args.agent_version,
        deployment_id=args.deployment_id,
        init_version=args.init_version,
        total_resources=resource_quantity_from_args(args),
        image_runtime=DockerImageRuntime(
            docker_binary=args.docker_binary,
            dry_run=False,
        ),
        max_concurrent_image_pulls=args.max_concurrent_image_pulls,
        node_control_bearer_token=read_required_token_file(
            args.node_control_bearer_token_file,
            "node control bearer token",
        ),
        telemetry=telemetry,
    )
    host, port = server.server_address
    print(f"Serving direct-runsc node agent on http://{host}:{port}")
    print(f"Direct state root: {state_root}")
    print(
        "Direct image cache root: "
        f"{args.image_cache_root or state_root / 'image-cache'}"
    )
    print(f"Storage-native mount root: {args.volume_mount_root}")
    print(f"Storage-native service: {args.storage_native_socket}")
    print(f"Runtime compatibility: {service.provisioner.runtime_compatibility_sha256}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping direct-runsc node agent.")
    finally:
        server.server_close()
        service.stop()
        telemetry.shutdown()
    return 0


def cmd_serve_model_relay(args: argparse.Namespace) -> int:
    from aiohttp import web

    config = load_config(args)
    telemetry = telemetry_from_config(config, "ucloud-sandboxes-model-relay")
    gateway_url = f"http://127.0.0.1:{config.gateway_port}"
    gateway_token = read_required_token_file(
        config.gateway_token_file(), "gateway bearer token"
    )

    async def accepted_notifier(relay_request: RelayRequest) -> str | None:
        return await asyncio.to_thread(
            _post_gateway_sandbox_lifecycle,
            gateway_url,
            gateway_token,
            relay_request,
            action="park",
        )

    async def result_notifier(relay_request: RelayRequest) -> str | None:
        return await asyncio.to_thread(
            _post_gateway_sandbox_lifecycle,
            gateway_url,
            gateway_token,
            relay_request,
            action="wake",
        )

    app = create_model_relay_app(
        sandbox_bearer_token=read_required_token_file(
            config.relay_sandbox_token_file(), "sandbox bearer token"
        ),
        worker_bearer_token=read_required_token_file(
            config.relay_worker_token_file(), "worker bearer token"
        ),
        request_timeout_seconds=config.relay_request_timeout_seconds,
        worker_poll_timeout_seconds=30.0,
        worker_lease_seconds=config.relay_worker_lease_seconds,
        completed_request_retention_seconds=(
            config.relay_completed_request_retention_seconds
        ),
        max_inflight_requests=DEFAULT_MAX_INFLIGHT_REQUESTS,
        max_inflight_requests_per_rollout=(DEFAULT_MAX_INFLIGHT_REQUESTS_PER_ROLLOUT),
        max_inflight_bytes=DEFAULT_MAX_INFLIGHT_BYTES,
        max_completed_bytes=DEFAULT_MAX_COMPLETED_BYTES,
        state_path=config.relay_state_file(),
        accepted_notifier=accepted_notifier,
        result_notifier=result_notifier,
        telemetry=telemetry,
    )

    async def shutdown_telemetry(_app: object) -> None:
        await asyncio.to_thread(telemetry.shutdown)

    app.on_cleanup.append(shutdown_telemetry)
    print(f"Serving model relay on http://{args.host}:{config.relay_port}")
    web.run_app(app, host=args.host, port=config.relay_port, print=None)
    return 0


class _RejectControlRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        return None


def _post_bounded_json(
    base_url: str,
    path: str,
    payload: dict[str, Any],
    *,
    bearer_token: str | None,
    invalid_url_error: str,
    empty_token_error: str,
    timeout_seconds: float,
    response_name: str,
) -> tuple[dict[str, Any], Any]:
    base_url = str(base_url).strip().rstrip("/")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(invalid_url_error)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if bearer_token is not None:
        bearer_token = bearer_token.strip()
        if not bearer_token:
            raise ValueError(empty_token_error)
        headers["Authorization"] = f"Bearer {bearer_token}"
    inject(headers)
    req = Request(
        f"{base_url}{path}",
        data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with build_opener(_RejectControlRedirects()).open(
        req,
        timeout=timeout_seconds,
    ) as response:
        body = response.read(_MAX_CONTROL_RESPONSE_BYTES + 1)
        if len(body) > _MAX_CONTROL_RESPONSE_BYTES:
            raise ValueError(f"{response_name} response exceeds 1 MiB")
        decoded = json.loads(body.decode("utf-8")) if body else {}
        if not isinstance(decoded, dict):
            raise ValueError(f"{response_name} response must be a JSON object")
        return decoded, response.headers


def _delete_bounded_json(
    base_url: str,
    path: str,
    *,
    bearer_token: str | None,
    invalid_url_error: str,
    empty_token_error: str,
    timeout_seconds: float,
    response_name: str,
) -> tuple[dict[str, Any], Any]:
    base_url = str(base_url).strip().rstrip("/")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(invalid_url_error)
    headers = {"Accept": "application/json"}
    if bearer_token is not None:
        bearer_token = bearer_token.strip()
        if not bearer_token:
            raise ValueError(empty_token_error)
        headers["Authorization"] = f"Bearer {bearer_token}"
    inject(headers)
    req = Request(
        f"{base_url}{path}",
        headers=headers,
        method="DELETE",
    )
    with build_opener(_RejectControlRedirects()).open(
        req,
        timeout=timeout_seconds,
    ) as response:
        body = response.read(_MAX_CONTROL_RESPONSE_BYTES + 1)
        if len(body) > _MAX_CONTROL_RESPONSE_BYTES:
            raise ValueError(f"{response_name} response exceeds 1 MiB")
        decoded = json.loads(body.decode("utf-8")) if body else {}
        if not isinstance(decoded, dict):
            raise ValueError(f"{response_name} response must be a JSON object")
        return decoded, response.headers


def _post_gateway_sandbox_lifecycle(
    gateway_url: str,
    bearer_token: str | None,
    relay_request: RelayRequest,
    *,
    action: str,
) -> str | None:
    if action not in {"park", "wake"}:
        raise ValueError("unsupported relay sandbox lifecycle action")
    if relay_request.sandbox_id is None:
        return
    if relay_request.sandbox_generation is None:
        raise ValueError("relay sandbox lifecycle binding has no generation")
    for attempt in range(101):
        try:
            _payload, headers = _post_bounded_json(
                gateway_url,
                f"/v1/sandboxes/{quote(relay_request.sandbox_id, safe='')}/{action}",
                {
                    "generation": relay_request.sandbox_generation,
                    "operation_id": f"relay-{action}:{relay_request.request_id}",
                    "rollout_id": relay_request.rollout_id,
                    "request_id": relay_request.request_id,
                    "request_created_at": relay_request.created_at,
                },
                bearer_token=bearer_token,
                invalid_url_error="gateway URL is invalid",
                empty_token_error="gateway bearer token cannot be empty",
                timeout_seconds=600.0,
                response_name="gateway lifecycle",
            )
            break
        except HTTPError as exc:
            # The node's independent idle parker can win the lifecycle fence
            # between enqueue and this explicit park. Once it finishes, this
            # idempotent retry observes the parked state and lets the gateway
            # durably record the request transition.
            if exc.code != 409 or attempt >= 100:
                raise
            error_body = exc.read(_MAX_CONTROL_RESPONSE_BYTES + 1)
            try:
                error_payload = json.loads(error_body.decode("utf-8"))
                error_message = (
                    str(error_payload.get("error") or "").strip()
                    if isinstance(error_payload, dict)
                    else ""
                )
            except (UnicodeDecodeError, json.JSONDecodeError):
                error_message = ""
            exc.close()
            if "cannot survive park" in error_message:
                raise RuntimeError(error_message) from exc
            time.sleep(0.05)
    transport_epoch = headers.get("X-UCloud-Sandbox-Transport-Epoch", "").strip()
    return transport_epoch or None


def cmd_init_vm(args: argparse.Namespace) -> int:
    config = load_config(args)
    provider = compute_provider_from_args(args, config)
    if not provider.scope_id:
        raise ValueError("provider.scope_id is required")
    instance = provider.retrieve_instance(args.job_id, include_updates=True)
    plan = provider.bootstrap_access(instance)
    options = vm_init_options_for_job(
        config,
        plan.instance,
        args.role,
        package_spec=str(args.package_spec.expanduser().resolve()),
    )

    result: dict[str, Any] = {
        "projectId": provider.scope_id,
        "provider": {"kind": provider.kind, "scopeId": provider.scope_id},
        "job": vm_job_to_dict(plan.instance),
        "sshCommand": plan.command,
        "runnable": plan.runnable,
        "reason": plan.reason,
        "options": vm_init_options_to_dict(options),
        "execute": args.execute,
    }

    if args.execute:
        if not plan.runnable or not plan.command:
            raise ValueError(plan.reason)
        effective_options = options
        stage_result = stage_vm_init_package_over_ssh(
            plan.command,
            options,
            timeout_seconds=max(1, args.timeout_seconds),
            private_key_file=args.ssh_private_key_file,
        )
        result["packageStage"] = {
            "localPath": str(stage_result.local_path),
            "remotePath": stage_result.remote_path,
            "command": list(stage_result.command),
            "returncode": stage_result.returncode,
            "reused": stage_result.reused,
        }
        if stage_result.returncode != 0:
            raise ValueError(
                f"remote package staging failed with exit code {stage_result.returncode}"
            )
        effective_options = replace(
            options,
            package_spec=stage_result.remote_path,
            package_sha256=stage_result.package_sha256,
        )
        run_result = run_init_over_ssh(
            plan.command,
            render_vm_init_script(effective_options),
            timeout_seconds=max(1, args.timeout_seconds),
            private_key_file=args.ssh_private_key_file,
        )
        result["run"] = {
            "command": list(run_result.command),
            "returncode": run_result.returncode,
            "initPhasesMs": dict(run_result.phase_durations_ms),
            "initTotalMs": run_result.total_duration_ms,
        }
        if run_result.returncode != 0:
            raise ValueError(
                f"remote init failed with exit code {run_result.returncode}"
            )

    if args.output == "json":
        print_json(result)
    else:
        print(f"Provider: {provider.kind} ({provider.scope_id})")
        print(f"Job: {plan.instance.id}")
        print(f"State: {plan.instance.state}")
        print(f"SSH enabled: {plan.instance.ssh_enabled}")
        print(f"SSH command: {plan.command or ''}")
        print(f"Deployment: {options.deployment_id}")
        print(f"Runnable: {plan.runnable}")
        print(f"Reason: {plan.reason}")
        print(f"Mode: {'execute' if args.execute else 'dry-run'}")
        if args.execute and "run" in result:
            print(f"Remote init exit code: {result['run']['returncode']}")
        if not args.execute:
            print(
                "Dry-run only. Re-run with --execute to run the init script over SSH."
            )
    return 0


def cmd_ensure_ucloud_ssh_key(args: argparse.Namespace) -> int:
    config = load_config(args)
    public_key = read_public_ssh_key_file(args.public_key_file)
    client = UCloudClient(SessionStore(Path(ucloud_settings(config).session_file)))

    existing = find_ucloud_ssh_key(client.browse_ssh_keys(), public_key)
    response: dict[str, Any] | None = None
    create_timeout = False
    if existing is None:
        try:
            response = client.create_ssh_key(title=args.title, key=public_key)
        except TimeoutError:
            create_timeout = True
        existing = find_ucloud_ssh_key(client.browse_ssh_keys(), public_key)
        if existing is None and create_timeout:
            raise UCloudError(
                "Timed out while creating the UCloud SSH key, and a follow-up browse "
                "did not find it."
            )

    result = {
        "present": existing is not None,
        "created": response is not None or create_timeout,
        "timedOutAfterCreate": create_timeout,
        "id": existing.get("id") if isinstance(existing, dict) else None,
        "title": (
            existing.get("specification", {}).get("title")
            if isinstance(existing.get("specification"), dict)
            else None
        )
        if isinstance(existing, dict)
        else None,
        "response": response or {},
    }
    if args.output == "json":
        print_json(result)
    else:
        status = "created" if result["created"] else "already present"
        print(
            f"UCloud SSH key {status}: {result['id'] or ''} {result['title'] or ''}".rstrip()
        )
        if create_timeout:
            print("Create request timed out, but follow-up browse found the key.")
    return 0


def cmd_vm_network_attachment(args: argparse.Namespace) -> int:
    config = load_config(args)
    private_network_id = (
        args.private_network_id or ucloud_settings(config).private_network_id
    )
    if not private_network_id:
        raise ValueError(
            "private network id is required via --private-network-id or config."
        )
    hostname_prefix = args.hostname_prefix or "sandbox-node"
    hostname = args.hostname
    if not hostname:
        seed = args.hostname_seed or private_network_id
        hostname = stable_hostname(seed, prefix=hostname_prefix)
    attachment = PrivateNetworkAttachment(
        network_id=private_network_id,
        hostname=hostname,
    )
    fragment = apply_private_network_attachment({}, attachment)
    result = {
        "privateNetworkId": attachment.network_id,
        "hostname": attachment.hostname,
        "resource": attachment.to_resource(),
        "jobFragment": fragment,
    }
    if args.output == "json":
        print_json(result)
    else:
        print(f"Private network: {attachment.network_id}")
        print(f"Hostname: {attachment.hostname}")
        print_json(fragment)
    return 0


def cmd_vm_public_link_attachment(args: argparse.Namespace) -> int:
    config = load_config(args)
    public_link_id = (
        args.public_link_id or ucloud_settings(config).gateway_public_link_id
    )
    if not public_link_id:
        raise ValueError(
            "public link id is required via --public-link-id or "
            "provider.gateway_public_link_id."
        )
    port = (
        args.port
        if args.port is not None
        else ucloud_settings(config).gateway_public_link_port
        or DEFAULT_PUBLIC_LINK_PORT
    )
    attachment = PublicLinkAttachment(
        link_id=public_link_id,
        port=port,
    )
    fragment = apply_public_link_attachment({}, attachment)
    result = {
        "publicLinkId": attachment.link_id,
        "port": attachment.port,
        "resource": attachment.to_resource(),
        "jobFragment": fragment,
    }
    if args.output == "json":
        print_json(result)
    else:
        print(f"Public link: {attachment.link_id}")
        print(f"Port: {attachment.port}")
        print_json(fragment)
    return 0


def cmd_registry_prune(args: argparse.Namespace) -> int:
    config = load_config(args)
    client = RegistryClient(config.registry_url)
    usage_store = RegistryUsageStore(config.registry_usage_file())
    usage_snapshot = usage_store.snapshot()
    usage_records = usage_snapshot.records
    plan = registry_prune_plan(
        client,
        keep_per_repository=config.registry_keep_per_repository,
        repository_prefix=args.repository_prefix,
        max_age_days=config.registry_retention_days,
        usage_records=usage_records,
        active_leases=usage_snapshot.leases,
        usage_generation=usage_snapshot.generation,
    )
    plan["execute"] = bool(args.execute)
    plan["usage_file"] = str(config.registry_usage_file())
    plan["image_file"] = str(config.image_file())
    if args.execute:
        deleted = []
        for attempt in range(3):
            usage_snapshot = usage_store.snapshot()
            usage_records = usage_snapshot.records
            records = list_registry_tags(
                client,
                repository_prefix=args.repository_prefix,
            )
            records = apply_registry_usage(records, usage_records)
            candidates = select_prune_candidates(
                records,
                keep_per_repository=config.registry_keep_per_repository,
                max_age_days=config.registry_retention_days,
                use_last_used_at=True,
                active_leases=usage_snapshot.leases,
            )
            try:
                deleted = execute_registry_prune(
                    client,
                    candidates,
                    usage_store=usage_store,
                    expected_usage_generation=usage_snapshot.generation,
                    all_records=records,
                )
                break
            except RegistryUsageGenerationChanged:
                if attempt == 2:
                    raise
                continue
        plan["deleted"] = [item.to_dict() for item in deleted]
        plan["usage_generation"] = usage_snapshot.generation
        plan["active_lease_count"] = len(usage_snapshot.leases)
        removed = _remove_image_records_for_registry_tags(
            config.image_file(),
            {(record.repository, record.tag) for record in deleted},
        )
        removed.extend(
            _remove_stale_private_build_image_records(
                config.image_file(),
                client,
            )
        )
        plan["removed_image_records"] = [
            item.to_dict() for item in _dedupe_image_records(removed)
        ]
    print_json(plan)
    return 0


def _remove_image_records_for_registry_tags(
    image_file: Path,
    registry_tags: set[tuple[str, str]],
) -> list[ImageRecord]:
    if not registry_tags:
        return []
    store = ImageStore(image_file)
    records = store.load()
    tags_to_remove = [
        record.tag
        for record in records.values()
        if registry_repository_tag_from_image_ref(record.tag) in registry_tags
    ]
    return store.delete_by_tags(tags_to_remove)


def _remove_stale_private_build_image_records(
    image_file: Path,
    client: RegistryClient,
) -> list[ImageRecord]:
    store = ImageStore(image_file)
    records = store.load()
    tags_to_remove: list[str] = []
    for record in records.values():
        if not _image_record_is_pushed_private_build(record, client.base_url):
            continue
        parsed = registry_repository_tag_from_image_ref(record.tag)
        if parsed is None:
            continue
        try:
            exists = client.tag_exists(*parsed)
        except (OSError, ValueError, RegistryRequestError):
            continue
        if not exists:
            tags_to_remove.append(record.tag)
    return store.delete_by_tags(tags_to_remove)


def _image_record_is_pushed_private_build(
    record: ImageRecord,
    registry_url: str,
) -> bool:
    return bool(
        record.pushed
        and record.source.startswith("build:")
        and _image_ref_uses_private_registry(record.tag, registry_url)
    )


def _image_ref_uses_private_registry(image_ref: str, registry_url: str) -> bool:
    host = registry_host_from_image_ref(image_ref)
    if not host:
        return False
    registry_host = urlparse(registry_url).netloc
    allowed = {
        "ucloud-sandbox-registry:5000",
        "localhost:5000",
        "127.0.0.1:5000",
    }
    if registry_host:
        allowed.add(registry_host)
    return host in allowed


def _dedupe_image_records(records: list[ImageRecord]) -> list[ImageRecord]:
    deduped: dict[str, ImageRecord] = {}
    for record in records:
        deduped[record.id] = record
    return [deduped[key] for key in sorted(deduped)]


def cmd_submit_vm(args: argparse.Namespace) -> int:
    config = load_config(args)
    if not ucloud_settings(config).project_id:
        raise ValueError("provider.scope_id is required")

    options, seed = vm_submission_options_from_args(args, config)
    payload = options.bulk_payload()
    result: dict[str, Any] = {
        "projectId": ucloud_settings(config).project_id,
        "execute": args.execute,
        "role": args.role,
        "seed": seed,
        "hostname": options.hostname,
        "privateUrl": f"http://{options.hostname}:8090",
        "nodeId": options.hostname,
        "nodeUrl": f"http://{options.hostname}:8090",
        "publicLinkId": options.public_link_id,
        "publicLinkPort": (
            options.public_link_port if options.public_link_id else None
        ),
        "fileMounts": [
            {"path": mount.path, "readOnly": mount.read_only}
            for mount in options.file_mounts
        ],
        "payload": payload,
    }

    if args.execute:
        client = UCloudClient(SessionStore(Path(ucloud_settings(config).session_file)))
        response = client.submit_jobs(ucloud_settings(config).project_id, payload)
        result["response"] = response
        job_ids = submitted_job_ids(response)
        result["jobIds"] = job_ids

    if args.output == "json":
        print_json(result)
    else:
        print(f"Project: {ucloud_settings(config).project_id}")
        print(f"Role: {args.role}")
        print(f"Name: {options.name}")
        print(f"Hostname: {options.hostname}")
        print(f"Private network: {options.private_network_id or ''}")
        print(f"Public link: {options.public_link_id or ''}")
        if options.public_link_id:
            print(f"Public link port: {options.public_link_port}")
        if options.file_mounts:
            print("File mounts:")
            for mount in options.file_mounts:
                mode = "ro" if mount.read_only else "rw"
                print(f"- {mount.path} ({mode})")
        print(f"Application: {options.application.name}:{options.application.version}")
        print(
            "Product: "
            f"{options.product.provider}/{options.product.category}/{options.product.id}"
        )
        print(f"Disk: {options.disk_gb} GB")
        print(f"SSH enabled: {options.ssh_enabled}")
        print(f"Mode: {'execute' if args.execute else 'dry-run'}")
        if args.execute:
            job_ids = result.get("jobIds", [])
            print(
                f"Submitted job ids: {', '.join(job_ids) if job_ids else '(none returned)'}"
            )
            if job_ids:
                if options.ssh_enabled:
                    init_role = "builder" if args.role == "builder" else "sandbox"
                    print(
                        "Next: "
                        "ucloud-sandboxes init-vm "
                        f"--config {args.config} {job_ids[0]} "
                        f"--role {init_role} --package-spec <node-bundle>"
                    )
                else:
                    print(
                        "Next: wait for the VM to start, then use the supported "
                        "UCloud VM access channel for post-boot init."
                    )
        else:
            print_json(payload)
            print("Dry-run only. Re-run with --execute to submit the VM job.")
    return 0


def cmd_open_vm_web(args: argparse.Namespace) -> int:
    config = load_config(args)
    if not ucloud_settings(config).project_id:
        raise ValueError("provider.scope_id is required")
    if args.port < 1 or args.port > 65535:
        raise ValueError("port must be in [1, 65535].")
    if args.rank < 0:
        raise ValueError("rank cannot be negative.")

    client = UCloudClient(SessionStore(Path(ucloud_settings(config).session_file)))
    response = client.open_interactive_session(
        ucloud_settings(config).project_id,
        args.job_id,
        session_type="WEB",
        rank=args.rank,
        port=args.port,
    )
    if args.output == "json":
        print_json(response)
    else:
        print(
            f"Opened VM web session for job {args.job_id} rank {args.rank} port {args.port}."
        )
        for item in response.get("responses", []):
            session = item.get("session") if isinstance(item, dict) else None
            if isinstance(session, dict) and session.get("redirectClientTo"):
                print(f"URL: {session['redirectClientTo']}")
    return 0


def cmd_deploy_all_in_one(args: argparse.Namespace) -> int:
    config = load_config(args)
    settings = ucloud_settings(config)
    if not settings.project_id:
        raise ValueError("provider.scope_id is required")
    if not settings.private_network_id:
        raise ValueError("provider.private_network_id is required")

    client: UCloudClient | None = None

    def get_client() -> UCloudClient:
        nonlocal client
        if client is None:
            client = UCloudClient(SessionStore(Path(settings.session_file)))
        return client

    payload: dict[str, Any] | None = None

    def get_payload() -> dict[str, Any]:
        nonlocal payload
        if payload is None:
            payload = get_client().retrieve_job(
                settings.project_id,
                args.job_id,
                include_updates=True,
            )
        return payload

    ssh_command = args.ssh_command
    if not ssh_command and args.execute:
        init_plan = bootstrap_access_from_payload(get_payload())
        if not init_plan.runnable or not init_plan.command:
            raise ValueError(init_plan.reason)
        ssh_command = init_plan.command

    plan = AllInOneDeployPlan(
        job_id=args.job_id,
        config=config,
        local_wheel=args.wheel.expanduser().resolve(),
        local_direct_runsc=args.direct_runsc.expanduser().resolve(),
        local_managed_init=args.managed_init.expanduser().resolve(),
        local_storage_native_manifest=(
            args.storage_native_manifest.expanduser().resolve()
        ),
    )
    script = render_remote_deploy_script(plan)

    result: dict[str, Any] = {
        "plan": plan.to_dict(),
        "sshCommand": ssh_command,
        "copySession": not args.no_copy_session,
        "openPublicLinks": not args.no_open_public_links,
        "execute": args.execute,
        "stagedFiles": [],
        "registeredSshKey": None,
        "openWeb": [],
    }

    if args.output == "script":
        print(script, end="" if script.endswith("\n") else "\n")
        return 0

    if args.execute:
        if not ssh_command:
            raise ValueError(
                "--ssh-command is required when UCloud job updates do not expose SSH."
            )
        timeout = max(1, int(args.timeout_seconds))
        staged_wheel = stage_file_over_ssh(
            ssh_command,
            plan.local_wheel,
            plan.remote_wheel_path,
            timeout_seconds=timeout,
            private_key_file=args.ssh_private_key_file,
        )
        result["stagedFiles"].append(
            {
                "localPath": str(plan.local_wheel),
                "remotePath": plan.remote_wheel_path,
                "result": staged_wheel.to_dict(),
            }
        )
        if plan.local_direct_runsc is not None:
            staged_direct_runsc = stage_file_over_ssh(
                ssh_command,
                plan.local_direct_runsc,
                plan.remote_direct_runsc_path,
                mode="0755",
                timeout_seconds=timeout,
                private_key_file=args.ssh_private_key_file,
            )
            result["stagedFiles"].append(
                {
                    "localPath": str(plan.local_direct_runsc),
                    "remotePath": plan.remote_direct_runsc_path,
                    "result": staged_direct_runsc.to_dict(),
                }
            )
        if plan.local_managed_init is not None:
            staged_managed_init = stage_file_over_ssh(
                ssh_command,
                plan.local_managed_init,
                plan.remote_managed_init_path,
                mode="0755",
                timeout_seconds=timeout,
                private_key_file=args.ssh_private_key_file,
            )
            result["stagedFiles"].append(
                {
                    "localPath": str(plan.local_managed_init),
                    "remotePath": plan.remote_managed_init_path,
                    "result": staged_managed_init.to_dict(),
                }
            )
        if plan.local_storage_native_manifest is not None:
            from .deploy import storage_native_build_artifacts

            storage_artifacts = storage_native_build_artifacts(
                plan.local_storage_native_manifest
            )
            for local_path, remote_path, mode in (
                (
                    storage_artifacts.backend,
                    plan.remote_storage_native_backend_path,
                    "0755",
                ),
                (
                    storage_artifacts.manifest,
                    plan.remote_storage_native_manifest_path,
                    "0644",
                ),
                (
                    storage_artifacts.license,
                    plan.remote_storage_native_license_path,
                    "0644",
                ),
            ):
                staged_storage_file = stage_file_over_ssh(
                    ssh_command,
                    local_path,
                    remote_path,
                    mode=mode,
                    timeout_seconds=timeout,
                    private_key_file=args.ssh_private_key_file,
                )
                result["stagedFiles"].append(
                    {
                        "localPath": str(local_path),
                        "remotePath": remote_path,
                        "result": staged_storage_file.to_dict(),
                    }
                )
        if not args.no_copy_session:
            local_session = Path(settings.session_file).expanduser()
            staged_session = stage_file_over_ssh(
                ssh_command,
                local_session,
                plan.staged_session_file,
                mode="0600",
                timeout_seconds=timeout,
                private_key_file=args.ssh_private_key_file,
            )
            result["stagedFiles"].append(
                {
                    "localPath": str(local_session),
                    "remotePath": plan.staged_session_file,
                    "result": staged_session.to_dict(),
                }
            )
        remote_run = run_remote_script_over_ssh(
            ssh_command,
            script,
            timeout_seconds=timeout,
            private_key_file=args.ssh_private_key_file,
        )
        result["remoteRun"] = remote_run.to_dict()

        public_key = read_remote_text_over_ssh(
            ssh_command,
            plan.init_authorized_key_file,
            timeout_seconds=timeout,
            private_key_file=args.ssh_private_key_file,
        ).strip()
        existing = find_ucloud_ssh_key(get_client().browse_ssh_keys(), public_key)
        response: dict[str, Any] | None = None
        create_timeout = False
        if existing is None:
            try:
                response = get_client().create_ssh_key(
                    title=args.ssh_key_title
                    or f"ucloud-sandboxes gateway init {config.deployment_id}",
                    key=public_key,
                )
            except TimeoutError:
                create_timeout = True
            existing = find_ucloud_ssh_key(get_client().browse_ssh_keys(), public_key)
            if existing is None and create_timeout:
                raise UCloudError(
                    "Timed out while creating the UCloud SSH key, and a follow-up "
                    "browse did not find it."
                )
        result["registeredSshKey"] = {
            "present": existing is not None,
            "created": response is not None or create_timeout,
            "timedOutAfterCreate": create_timeout,
            "id": existing.get("id") if isinstance(existing, dict) else None,
            "title": (
                existing.get("specification", {}).get("title")
                if isinstance(existing.get("specification"), dict)
                else None
            )
            if isinstance(existing, dict)
            else None,
        }

        if not args.no_open_public_links:
            for port in (config.gateway_port, config.relay_port):
                response = get_client().open_interactive_session(
                    settings.project_id,
                    args.job_id,
                    session_type="WEB",
                    rank=0,
                    port=port,
                )
                result["openWeb"].append({"port": port, "response": response})

    if args.output == "json":
        print_json(result)
    else:
        print(f"Project: {settings.project_id}")
        print(f"Job: {args.job_id}")
        print(f"Deployment: {config.deployment_id}")
        print(f"Version: {plan.package_version}")
        print(f"Wheel: {plan.local_wheel}")
        print(f"Remote wheel: {plan.remote_wheel_path}")
        print(f"Private gateway host: {config.gateway_private_host}")
        print(f"Worker registry: {config.registry_worker_url}")
        print(f"Mode: {'execute' if args.execute else 'dry-run'}")
        if args.execute:
            print(
                "Services converged: gateway, relay, registry, registry prune, registry GC, autoscaler"
            )
            if result["registeredSshKey"]:
                key = result["registeredSshKey"]
                print(f"Gateway init SSH key: {key.get('id') or '(present)'}")
            opened = [str(item["port"]) for item in result["openWeb"]]
            if opened:
                print(f"Opened VM web ports: {', '.join(opened)}")
        else:
            print(
                "Dry-run only. Re-run with --execute to stage files and restart services."
            )
            print("Use --output script to inspect the exact remote install script.")
    return 0


def cmd_heartbeats(args: argparse.Namespace) -> int:
    config = load_config(args)
    control_state_file = config.control_state_file()
    heartbeats = ControlStateStore(control_state_file).load_heartbeats()
    nodes = [heartbeat_to_dict(heartbeats[job_id]) for job_id in sorted(heartbeats)]
    if args.output == "json":
        print_json({"controlStateFile": str(control_state_file), "nodes": nodes})
    else:
        print(f"Control state: {control_state_file}")
        if not nodes:
            print("No heartbeats found.")
        for node in nodes:
            total = node.get("total_resources", {})
            used = node.get("used_resources", {})
            print(
                f"- node={node['node_id']} job={node['job_id']} "
                f"active={node['active_sandboxes']} "
                f"url={node.get('node_url') or ''} "
                f"capabilities={','.join(node.get('capabilities', []))} "
                f"used={resource_summary(used)} "
                f"total={resource_summary(total)} "
                f"updated={node['updated_at']}"
            )
    return 0


def reject_mutating_jobs_fixture(
    args: argparse.Namespace,
    *,
    execution_requested: bool,
) -> None:
    if execution_requested and getattr(args, "jobs_file", None) is not None:
        raise ValueError(
            "--jobs-file is dry-run only and cannot be combined with --execute"
        )


def cmd_autoscaler(args: argparse.Namespace) -> int:
    config = load_config(args)
    telemetry = telemetry_from_config(config, "ucloud-sandboxes-autoscaler")
    provider = compute_provider_from_args(args, config)
    if not provider.scope_id:
        raise ValueError("provider.scope_id is required")
    route_file = config.routing_file()
    metrics_file = config.metrics_path()
    metrics_store = MetricsStore(metrics_file)
    interval = config.autoscaler_interval_seconds
    cycle = 0
    observed_vm_keys: dict[str, tuple[object, ...]] = {}
    execution_requested = bool(args.execute)
    reject_mutating_jobs_fixture(args, execution_requested=execution_requested)
    provider_state = (
        AutoscalerStateStore(config.autoscaler_state_file())
        if execution_requested
        else None
    )
    process_lock: AutoscalerProcessLock | None = (
        provider_state.process_lock() if provider_state is not None else None
    )
    bootstrap_coordinator = (
        _VmBootstrapCoordinator(
            max(1, config.autoscaler_max_init_per_cycle),
            metrics_store,
            telemetry=telemetry,
        )
        if (args.execute and not args.once and config.autoscaler_max_init_per_cycle > 0)
        else None
    )

    def assert_process_fence() -> None:
        if process_lock is None or not process_lock.held:
            raise AutoscalerStateError("autoscaler controller lock is not held")

    try:
        while True:
            cycle += 1
            if process_lock is not None and not process_lock.held:
                process_lock.acquire(blocking=False)
            if (
                args.once
                and execution_requested
                and process_lock is not None
                and not process_lock.held
            ):
                raise AutoscalerStateError(
                    "another local autoscaler process holds the controller lock"
                )
            controller_active = bool(process_lock is not None and process_lock.held)
            routing_store = RoutingStore(route_file)
            routing_state = routing_store.load()
            pending_snapshot = list(routing_state.pending.values())
            capacity_pending_snapshot = [
                item for item in pending_snapshot if item.is_capacity_demand
            ]
            pending_image_build_snapshot = list(routing_state.image_builds.values())
            prepared_builder_snapshot = list(routing_state.prepared_builders.values())
            program_request_snapshot = routing_store.program_requests_readonly()
            demand = sandbox_demand_from_routing_state(routing_state)
            route_reservations = sandbox_route_reservations(
                routing_state.sandboxes.values()
            )
            pending_image_builds = len(pending_image_build_snapshot)
            prepared_builder_count = sum(
                item.count for item in prepared_builder_snapshot
            )
            with telemetry.span(
                "autoscaler.reconcile",
                attributes={
                    "autoscaler.cycle": cycle,
                    "autoscaler.controller_active": controller_active,
                    "autoscaler.execute": execution_requested,
                    "autoscaler.pending_sandboxes": len(pending_snapshot),
                    "autoscaler.pending_image_builds": pending_image_builds,
                },
            ) as reconcile_span:
                result = run_reconcile_cycle(
                    config,
                    args,
                    provider=provider,
                    demand=demand,
                    pending_image_builds=pending_image_builds,
                    prepared_builder_count=prepared_builder_count,
                    metrics_store=metrics_store,
                    provider_state=provider_state,
                    provider_mutations_allowed=controller_active,
                    route_reservations=route_reservations,
                    sandbox_routes=tuple(routing_state.sandboxes.values()),
                    program_requests=tuple(program_request_snapshot),
                    pending_wake_sandbox_ids={
                        item.sandbox_id.removeprefix("__wake__:")
                        for item in pending_snapshot
                        if item.sandbox_id.startswith("__wake__:")
                    },
                    bootstrap_coordinator=bootstrap_coordinator,
                    provider_fence=assert_process_fence,
                    telemetry=telemetry,
                )
                reconcile_span.set_attributes(
                    {
                        "autoscaler.create_count": len(
                            result.get("submittedJobIds", [])
                        ),
                        "autoscaler.stop_count": len(result.get("stopJobIds", [])),
                    }
                )
            removed_routes = []
            consumed_pending_demand = []
            consumed_pending_image_builds = []
            consumed_prepared_builders = []
            persisted_node_loss_demand = []
            if controller_active:
                destructive_job_ids = {
                    str(job_id)
                    for job_id in result.get("destructive_power_cycle_job_ids", [])
                }
                removed_routes = routing_store.delete_sandboxes_for_jobs_with_error(
                    destructive_job_ids,
                    terminal_error="node_lost",
                )
                route_cleanup_job_ids = set(result.get("prunedFinalHeartbeats", []))
                route_cleanup_job_ids.update(
                    str(job_id)
                    for job_id in result.get("definitelyTerminatedJobIds", [])
                )
                removed_routes.extend(
                    routing_store.delete_sandboxes_for_jobs(
                        route_cleanup_job_ids - destructive_job_ids
                    )
                )
                if args.execute:
                    effective_policy = config.policy
                    stale_route_grace_seconds = max(
                        effective_policy.heartbeat_ttl_seconds * 3,
                        effective_policy.heartbeat_ttl_seconds + 60,
                    )
                    active_route_job_ids = {
                        node.job_id
                        for node in result["rawNodes"]
                        if not node.job.is_final
                    }
                    active_route_node_ids = {
                        node.heartbeat.node_id
                        for node in result["rawNodes"]
                        if node.heartbeat is not None and node.heartbeat_fresh
                    }
                    removed_routes.extend(
                        routing_store.delete_stale_sandboxes(
                            active_job_ids=active_route_job_ids,
                            active_node_ids=active_route_node_ids,
                            older_than=utc_now()
                            - timedelta(seconds=stale_route_grace_seconds),
                        )
                    )
                if controller_active and result.get(
                    "sandboxCapacityOperationSucceeded"
                ):
                    consumed_pending_demand = routing_store.consume_pending_demand(
                        capacity_pending_snapshot
                    )
                if controller_active and result.get(
                    "builderCapacityOperationSucceeded"
                ):
                    consumed_pending_image_builds = (
                        routing_store.consume_pending_image_builds(
                            pending_image_build_snapshot
                        )
                    )
                    consumed_prepared_builders = (
                        routing_store.consume_prepared_builders(
                            prepared_builder_snapshot
                        )
                    )
                replacement_deficit = result["rawDecision"].resource_deficit
                if (
                    controller_active
                    and removed_routes
                    and not result.get("sandboxCapacityOperationSucceeded")
                    and (
                        replacement_deficit.vcpu > 0
                        or replacement_deficit.memory_mb > 0
                        or replacement_deficit.disk_mb > 0
                    )
                ):
                    for route in removed_routes:
                        if route.job_id not in destructive_job_ids:
                            continue
                        demand_id = (
                            f"__node_loss__:{route.job_id}:{route.sandbox_id}:"
                            f"{route.generation}"
                        )
                        routing_store.upsert_pending(
                            demand_id,
                            route.resources,
                            generation=route.generation,
                            operation_id=route.create_operation_id,
                            spec_hash=route.spec_hash,
                            failure_reason="node_lost_replacement",
                        )
                        pending = routing_store.get_pending(demand_id)
                        if pending is not None:
                            persisted_node_loss_demand.append(pending)
            result["cycle"] = cycle
            result["routeFile"] = str(route_file)
            result["metricsFile"] = str(metrics_file)
            result["autoscalerStateFile"] = (
                str(provider_state.path) if provider_state is not None else ""
            )
            result["controllerLockHeld"] = controller_active
            result["consumedPendingDemand"] = [
                item.to_dict() for item in consumed_pending_demand
            ]
            result["consumedPendingImageBuilds"] = [
                item.to_dict() for item in consumed_pending_image_builds
            ]
            result["consumedPreparedBuilders"] = [
                item.to_dict() for item in consumed_prepared_builders
            ]
            result["persistedNodeLossDemand"] = [
                item.to_dict() for item in persisted_node_loss_demand
            ]
            result["removedRoutes"] = [route.to_dict() for route in removed_routes]
            record_autoscaler_cycle(metrics_store, cycle=cycle, result=result)
            record_submitted_vm_metrics(metrics_store, cycle, result)
            record_observed_vm_metrics(metrics_store, cycle, result, observed_vm_keys)
            if args.output == "json":
                printable = dict(result)
                for key in (
                    "rawNodes",
                    "rawSandboxNodes",
                    "rawBuilderNodes",
                    "rawDecision",
                    "rawBuilderDecision",
                    "rawCreateIntents",
                    "rawBootstrapIntents",
                ):
                    printable.pop(key, None)
                print_json(printable)
            else:
                print(
                    f"Autoscaler cycle {cycle}: "
                    f"pending_resources={resource_summary(demand.pending_resources.to_dict())} "
                    f"prepared_resources={resource_summary(demand.prepared_resources.to_dict())} "
                    f"prepared_builders={prepared_builder_count}"
                )
                print_reconcile(
                    result["rawSandboxNodes"],
                    result["rawDecision"],
                    Path(result["controlStateFile"]),
                    result["rawCreateIntents"],
                    tuple(result["stopJobIds"]),
                    result,
                )
            sys.stdout.flush()
            if args.once:
                return 0
            if bootstrap_coordinator is not None:
                bootstrap_coordinator.wait_for_activity(interval)
            else:
                time.sleep(interval)
    finally:
        if bootstrap_coordinator is not None:
            # The provider fence must remain held until every submitted SSH
            # init attempt has returned. Otherwise a replacement controller
            # can retry the same durable attempt while this process still runs.
            bootstrap_coordinator.shutdown(wait=True)
        if process_lock is not None:
            process_lock.release()
        telemetry.shutdown()


def _post_node_drain(
    node_url: str,
    token: str,
    *,
    draining: bool = True,
    bearer_token: str | None = None,
    timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    return _post_bounded_json(
        node_url,
        "/v1/drain",
        {"token": token, "draining": draining},
        bearer_token=bearer_token,
        invalid_url_error="node heartbeat has an invalid node URL",
        empty_token_error="node control bearer token cannot be empty",
        timeout_seconds=timeout_seconds,
        response_name="node drain",
    )[0]


def _post_gateway_sandbox_migration(
    gateway_url: str,
    sandbox_id: str,
    *,
    bearer_token: str | None = None,
    timeout_seconds: float = 3600.0,
) -> dict[str, Any]:
    return _post_bounded_json(
        gateway_url,
        f"/v1/sandboxes/{quote(sandbox_id, safe='')}/migration",
        {},
        bearer_token=bearer_token,
        invalid_url_error="gateway control URL is invalid",
        empty_token_error="gateway control bearer token cannot be empty",
        timeout_seconds=timeout_seconds,
        response_name="gateway migration",
    )[0]


def _post_gateway_sandbox_detach(
    gateway_url: str,
    sandbox_id: str,
    *,
    bearer_token: str | None = None,
    timeout_seconds: float = 3600.0,
) -> dict[str, Any]:
    return _post_bounded_json(
        gateway_url,
        f"/v1/sandboxes/{quote(sandbox_id, safe='')}/detach",
        {},
        bearer_token=bearer_token,
        invalid_url_error="gateway control URL is invalid",
        empty_token_error="gateway control bearer token cannot be empty",
        timeout_seconds=timeout_seconds,
        response_name="gateway worker detach",
    )[0]


def _delete_gateway_sandbox(
    gateway_url: str,
    sandbox_id: str,
    *,
    bearer_token: str | None = None,
    timeout_seconds: float = 3600.0,
) -> dict[str, Any]:
    return _delete_bounded_json(
        gateway_url,
        f"/v1/sandboxes/{quote(sandbox_id, safe='')}",
        bearer_token=bearer_token,
        invalid_url_error="gateway control URL is invalid",
        empty_token_error="gateway control bearer token cannot be empty",
        timeout_seconds=timeout_seconds,
        response_name="gateway sandbox delete replay",
    )[0]


def _drain_response_acknowledges(
    response: dict[str, Any],
    *,
    token: str,
    draining: bool,
) -> bool:
    drain = response.get("drain")
    return bool(
        isinstance(drain, dict)
        and str(drain.get("token") or "").strip() == str(token).strip()
        and drain.get("draining") is draining
        and drain.get("admission_open") is (not draining)
    )


def _nodes_with_drain_admission_reopened(
    nodes: list[SandboxNode],
    job_ids: set[str],
) -> list[SandboxNode]:
    """Return a planning-only view where selected drain intents are canceled."""

    reopened: list[SandboxNode] = []
    for node in nodes:
        heartbeat = node.heartbeat
        if node.job_id not in job_ids or heartbeat is None:
            reopened.append(node)
            continue
        reopened.append(
            replace(
                node,
                heartbeat=replace(
                    heartbeat,
                    draining=False,
                    admission_open=True,
                ),
            )
        )
    return reopened


def _drain_intent_to_dict(intent: DrainIntent) -> dict[str, Any]:
    return {
        "deploymentId": intent.deployment_id,
        "jobId": intent.job_id,
        "role": intent.role,
        "token": intent.token,
        "state": intent.state,
        "updatedAt": intent.updated_at.isoformat(),
    }


def _stop_operation_has_safety_proof(
    provider_state: AutoscalerStateStore,
    operation: ProviderOperation,
) -> bool:
    if operation.kind != "stop" or len(operation.target_job_ids) != 1:
        return operation.kind != "stop"
    if operation.request.get("destructivePowerCycle") is True:
        return bool(
            operation.request.get("postStartSuspensionObserved") is True
            and isinstance(operation.request.get("routeCount"), int)
            and int(operation.request["routeCount"]) >= 0
        )
    if operation.request.get("unreachableStaleReady") is True:
        return bool(
            str(operation.request.get("unreachableReference") or "").strip()
            and operation.request.get("routeCount") == 0
            and operation.request.get("lastKnownActiveSandboxes") == 0
            and operation.request.get("lastHeartbeatSafeToStop") is True
        )
    token = str(operation.request.get("drainToken") or "").strip()
    if not token or operation.request.get("drainReady") is not True:
        return False
    intent = provider_state.get_drain_intent(
        operation.deployment_id,
        operation.target_job_ids[0],
    )
    return bool(
        intent is not None
        and intent.state == "active"
        and intent.token == token
        and intent.role == operation.role
    )


def apply_prepared_provider_operations(
    provider_state: AutoscalerStateStore,
    provider: ComputeProvider,
    *,
    source: str,
    allowed_kinds: set[str],
    allowed_stop_operation_ids: set[str] | None = None,
    telemetry: Telemetry | None = None,
) -> list[ProviderOperationOutcome]:
    telemetry = telemetry or _DISABLED_AUTOSCALER_TELEMETRY
    results: list[ProviderOperationOutcome] = []
    prepared_operations = provider_state.list_operations(states={"prepared"})
    # Release a destructively power-cycled VM before submitting its replacement
    # so a provider-side VM/core quota does not reject otherwise valid recovery.
    # Python's stable sort retains journal order within both classes.
    prepared_operations.sort(
        key=lambda operation: (
            0
            if operation.kind == "stop"
            and operation.request.get("destructivePowerCycle") is True
            else 1
        )
    )
    for prepared in prepared_operations:
        if prepared.kind not in allowed_kinds:
            continue
        if prepared.kind == "stop" and prepared.operation_id not in (
            allowed_stop_operation_ids or set()
        ):
            continue
        # Autoscaler stops require a fresh drain proof or a conservative
        # unreachable-empty lease proof.
        if prepared.kind == "stop" and not _stop_operation_has_safety_proof(
            provider_state, prepared
        ):
            continue
        submitting = provider_state.begin_provider_call(prepared.operation_id)
        try:
            with telemetry.span(
                f"provider.{submitting.kind}",
                attributes={
                    "provider.operation.id": submitting.operation_id,
                    "provider.operation.kind": submitting.kind,
                    "provider.role": submitting.role,
                    "provider.target_count": len(submitting.target_job_ids),
                },
            ) as provider_span:
                if submitting.kind == "create":
                    outcome = provider.create(submitting.request)
                else:
                    outcome = provider.terminate(submitting.target_job_ids)
                provider_span.set_attribute("provider.outcome", outcome.status)
        except Exception as exc:
            # Unknown adapter failures cannot prove whether the provider applied
            # the request. Adapters should normally return an uncertain result.
            operation = provider_state.mark_operation_uncertain(
                submitting.operation_id,
                error=str(exc),
            )
        else:
            if outcome.status == "rejected":
                operation = provider_state.mark_operation_failed(
                    submitting.operation_id,
                    error=outcome.error or "provider explicitly rejected the operation",
                    response=outcome.response,
                )
            elif outcome.status == "accepted":
                operation = provider_state.mark_operation_accepted(
                    submitting.operation_id,
                    response=outcome.response,
                    target_job_ids=outcome.instance_ids,
                )
            else:
                operation = provider_state.mark_operation_uncertain(
                    submitting.operation_id,
                    error=outcome.error
                    or "provider response did not prove whether the operation applied",
                )
        results.append(
            ProviderOperationOutcome.from_operation(operation, source=source)
        )
    return results


def _successful_create_operation_count(
    operation_results: list[ProviderOperationOutcome],
    role: str,
) -> int:
    relevant = [
        item
        for item in operation_results
        if item.kind == "create" and item.role == role
    ]
    job_ids = [job_id for item in relevant for job_id in item.job_ids]
    if (
        not relevant
        or not all(item.state in {"accepted", "recovered"} for item in relevant)
        or len(job_ids) != len(relevant)
        or len(set(job_ids)) != len(job_ids)
    ):
        return 0
    return len(job_ids)


def _sandbox_capacity_operation_succeeded(
    operation_results: list[ProviderOperationOutcome],
    resource_deficit: ResourceQuantity,
    default_node_resources: ResourceQuantity,
) -> bool:
    count = _successful_create_operation_count(operation_results, "sandbox")
    if count <= 0:
        return False
    created = ResourceQuantity(
        vcpu=default_node_resources.vcpu * count,
        memory_mb=default_node_resources.memory_mb * count,
        disk_mb=default_node_resources.disk_mb * count,
    )
    return resource_deficit.fits_within(created)


def _builder_capacity_operation_succeeded(
    operation_results: list[ProviderOperationOutcome],
    *,
    existing_builders: int,
    desired_builders: int,
) -> bool:
    count = _successful_create_operation_count(operation_results, "builder")
    return count > 0 and existing_builders + count >= desired_builders


def run_reconcile_cycle(
    config: DeploymentConfig,
    args: argparse.Namespace,
    *,
    provider: ComputeProvider | None = None,
    demand: SandboxDemand,
    pending_image_builds: int | None = None,
    prepared_builder_count: int | None = None,
    metrics_store: MetricsStore | None = None,
    provider_state: AutoscalerStateStore | None = None,
    provider_mutations_allowed: bool = False,
    route_reservations: dict[str, tuple[SandboxRoute, ...]] | None = None,
    sandbox_routes: tuple[SandboxRoute, ...] = (),
    program_requests: tuple[ProgramRequestState, ...] = (),
    pending_wake_sandbox_ids: set[str] | None = None,
    bootstrap_coordinator: _VmBootstrapCoordinator | None = None,
    provider_fence: Callable[[], None] | None = None,
    telemetry: Telemetry | None = None,
) -> dict[str, Any]:
    execution_requested = bool(args.execute)
    if (
        execution_requested
        and not provider_mutations_allowed
        and provider_state is None
    ):
        raise AutoscalerStateError(
            "provider mutations require the local autoscaler controller lock"
        )
    if execution_requested and provider_mutations_allowed and provider_state is None:
        raise AutoscalerStateError(
            "provider mutations require the autoscaler operation journal"
        )
    execution_authorized = bool(provider_mutations_allowed)

    def assert_provider_fence() -> None:
        if not provider_mutations_allowed:
            raise AutoscalerStateError("autoscaler controller lock is not held")
        if provider_fence is not None:
            provider_fence()

    provider = provider or compute_provider_from_args(args, config)
    telemetry = telemetry or _DISABLED_AUTOSCALER_TELEMETRY
    jobs = load_instances_for_plan(config, provider, args, telemetry=telemetry)
    operation_deployment_id = config.deployment_id or provider.scope_id
    provider_operation_results: list[ProviderOperationOutcome] = []
    create_visibility_guards: list[dict[str, Any]] = []
    blocked_create_roles: set[str] = set()
    if execution_authorized and provider_state is not None:
        provider_operation_results.extend(
            provider_state.reconcile_provider_inventory(jobs)
        )
        observed_job_ids = {job.id for job in jobs if job.id}
        allowed_kinds: set[str] = set()
        if args.execute:
            allowed_kinds.add("create")
        # Stops are replayed only after this cycle has refreshed every active
        # node drain intent below.
        replay_results = apply_prepared_provider_operations(
            provider_state,
            provider,
            source="prepared-replay",
            allowed_kinds=allowed_kinds,
            allowed_stop_operation_ids=set(),
            telemetry=telemetry,
        )
        provider_operation_results.extend(replay_results)
        for operation in provider_state.list_operations(
            kind="create",
            states={"uncertain", "accepted"},
        ):
            blocked_create_roles.add(operation.role)
        # A replayed create is absent from the inventory used for this plan.
        # Suppress another create for that role until the next exhaustive browse.
        for item in replay_results:
            if item.kind == "create" and item.state == "accepted":
                blocked_create_roles.add(item.role)
        for operation in provider_state.list_operations(
            kind="create",
            states={"accepted"},
        ):
            missing_job_ids = sorted(set(operation.target_job_ids) - observed_job_ids)
            if not missing_job_ids:
                continue
            blocked_create_roles.add(operation.role)
            create_visibility_guards.append(
                {
                    "operationId": operation.operation_id,
                    "role": operation.role,
                    "state": operation.state,
                    "missingJobIds": missing_job_ids,
                }
            )

    control_state_file = config.control_state_file()
    control_state = ControlStateStore(control_state_file)
    heartbeats = control_state.load_heartbeats()
    effective_policy = config.policy

    # Provider inventory may omit update history. A powered-off instance may
    # therefore appear running again after its ephemeral guest is replaced.
    # Normalized lost state and our own destructive stop journal are durable hints.
    # For a stale RUNNING node, retrieve its
    # full ordered updates once so the later RUNNING report cannot hide the
    # post-start suspension. A fresh heartbeat is not sufficient evidence of
    # continuity when its guest epoch differs from an owned route.
    loss_latched_job_ids: set[str] = set()
    if provider_state is not None:
        for operation in provider_state.list_operations(kind="stop"):
            if operation.request.get("destructivePowerCycle") is True:
                loss_latched_job_ids.update(operation.target_job_ids)
    jobs_by_id = {job.id: job for job in jobs}
    if not getattr(args, "jobs_file", None):
        for job in tuple(jobs):
            heartbeat = heartbeats.get(job.id)
            owned_routes = (route_reservations or {}).get(job.id, ())
            stale_heartbeat = bool(
                heartbeat is not None
                and not heartbeat.is_fresh(
                    utc_now(), effective_policy.heartbeat_ttl_seconds
                )
            )
            route_epoch_mismatch = bool(
                heartbeat is not None
                and heartbeat.node_epoch
                and any(
                    route.node_epoch and route.node_epoch != heartbeat.node_epoch
                    for route in owned_routes
                )
            )
            owns_routes = bool(owned_routes)
            should_retrieve_history = bool(
                job.state == "RUNNING"
                and not job.is_lost
                and job.id not in loss_latched_job_ids
                and owns_routes
                and (stale_heartbeat or route_epoch_mismatch)
            )
            if not should_retrieve_history:
                continue
            try:
                retrieved = provider.retrieve_instance(
                    job.id,
                    include_updates=True,
                )
            except ProviderError:
                continue
            jobs_by_id[job.id] = retrieved
        jobs = [jobs_by_id[job.id] for job in jobs]

    provider_job_ids = {job.id for job in jobs}
    orphaned_stale_heartbeat_job_ids = tuple(
        sorted(
            job_id
            for job_id, heartbeat in heartbeats.items()
            if job_id not in provider_job_ids
            and not (route_reservations or {}).get(job_id)
            and not heartbeat.is_fresh(
                utc_now(), effective_policy.heartbeat_ttl_seconds
            )
        )
    )
    if orphaned_stale_heartbeat_job_ids and execution_authorized:
        control_state.remove_heartbeats(orphaned_stale_heartbeat_job_ids)
        heartbeats = {
            job_id: heartbeat
            for job_id, heartbeat in heartbeats.items()
            if job_id not in orphaned_stale_heartbeat_job_ids
        }

    destructive_power_cycle_job_ids = tuple(
        sorted(job.id for job in jobs if job.id in loss_latched_job_ids or job.is_lost)
    )
    final_heartbeat_job_ids = tuple(
        sorted(job.id for job in jobs if job.is_final and job.id in heartbeats)
    )
    fenced_heartbeat_job_ids = tuple(
        sorted(
            set(final_heartbeat_job_ids)
            | (set(destructive_power_cycle_job_ids) & set(heartbeats))
        )
    )
    if fenced_heartbeat_job_ids and execution_authorized:
        control_state.remove_heartbeats(fenced_heartbeat_job_ids)
        heartbeats = {
            job_id: heartbeat
            for job_id, heartbeat in heartbeats.items()
            if job_id not in fenced_heartbeat_job_ids
        }
    heartbeats = apply_route_reservations_to_heartbeats(
        heartbeats,
        route_reservations or {},
    )
    nodes = merge_jobs_and_heartbeats(jobs, heartbeats, effective_policy)
    destructive_job_id_set = set(destructive_power_cycle_job_ids)
    nodes = [
        replace(node, permanently_lost=True)
        if node.job_id in destructive_job_id_set
        else node
        for node in nodes
    ]
    nodes = apply_route_reservations_to_nodes(
        nodes,
        route_reservations or {},
    )
    sandbox_nodes = sandbox_pool_nodes(nodes)
    builder_nodes = builder_pool_nodes(nodes)
    builder_pending = max(
        0,
        int(
            pending_image_builds
            if pending_image_builds is not None
            else getattr(args, "pending_image_builds", 0) or 0
        ),
    )
    builder_prepared = max(
        0,
        int(prepared_builder_count if prepared_builder_count is not None else 0),
    )
    active_image_builds = sum(
        max(0, node.heartbeat.active_image_builds)
        for node in builder_nodes
        if node.heartbeat is not None and node.heartbeat_fresh
    )
    build_warm_resources = build_activity_sandbox_warm_resources(
        active_image_builds=active_image_builds,
        pending_image_builds=builder_pending,
        prepared_builder_count=builder_prepared,
        policy=effective_policy,
    )
    sandbox_demand = demand_with_build_warm_resources(
        demand,
        build_warm_resources,
    )
    destructive_sandbox_job_ids = {
        node.job_id
        for node in sandbox_nodes
        if not node.job.is_final and node.job_id in set(destructive_power_cycle_job_ids)
    }
    lost_sandbox_routes = tuple(
        route
        for job_id in sorted(destructive_sandbox_job_ids)
        for route in (route_reservations or {}).get(job_id, ())
        if not is_portable_parked_route(route)
    )
    sandbox_demand = demand_with_lost_sandbox_replacement(
        sandbox_demand,
        lost_sandbox_routes,
    )
    live_scale_signals = None
    if metrics_store is not None:
        pressure_events = metrics_store.load_events(
            max_events=10_000,
            kinds=("node_heartbeat", "sandbox_create_busy"),
            since_seconds=max(
                effective_policy.live_pressure_window_seconds,
                effective_policy.create_pressure_window_seconds,
            ),
        )
        lifecycle_events = metrics_store.load_events(
            max_events=20_000,
            kinds=(
                "vm_submitted",
                "node_first_heartbeat",
                "sandbox_scheduled",
            ),
            since_seconds=effective_policy.provisioning_latency_lookback_seconds,
        )
        live_scale_signals = build_live_scale_signals(
            sorted(
                [*pressure_events, *lifecycle_events],
                key=lambda event: event.timestamp,
            ),
            effective_policy,
        )
    program_scale_signals = build_program_scale_signals(
        list(program_requests),
        list(sandbox_routes),
        effective_policy,
        pending_wake_sandbox_ids=pending_wake_sandbox_ids,
    )
    program_wake_plan = plan_shadow_wake_queue(
        list(program_requests),
        list(sandbox_routes),
        [
            WakeNodeCandidate(
                node_id=node.heartbeat.node_id,
                job_id=node.job_id,
                available=node.heartbeat.free_resources,
                total=node.heartbeat.total_resources,
                pressure=node_pressure_score(node.heartbeat),
            )
            for node in sandbox_nodes
            if node.is_schedulable and node.heartbeat is not None
        ],
    )
    decision = evaluate_scale(
        sandbox_nodes,
        sandbox_demand,
        effective_policy,
        live_signals=live_scale_signals,
        program_signals=program_scale_signals,
    )
    builder_decision = evaluate_builder_scale(
        builder_nodes,
        pending_builds=builder_pending,
        prepared_builders=builder_prepared,
        policy=effective_policy,
        max_builder_nodes=config.builder.max_nodes,
    )
    drain_workflow_enabled = bool(
        args.execute and execution_authorized and provider_state is not None
    )
    pending_drain_intents: list[DrainIntent] = []
    irreversible_stop_job_ids: set[str] = set()
    if drain_workflow_enabled:
        # Adopt both directions of the durable handshake before planning.  An
        # active drain is evaluated counterfactually as admission-open so a
        # demand increase can cancel it, while an already-started provider
        # termination is irreversible and must never reopen the node.
        pending_drain_intents = provider_state.pending_drain_intents(
            deployment_id=operation_deployment_id,
        )
        final_job_ids = {job.id for job in jobs if job.id and job.is_final}
        for intent in pending_drain_intents:
            if intent.job_id in final_job_ids:
                provider_state.retire_drain_intent(
                    deployment_id=intent.deployment_id,
                    job_id=intent.job_id,
                )
        pending_drain_intents = provider_state.pending_drain_intents(
            deployment_id=operation_deployment_id,
        )
        for operation in provider_state.list_operations(
            kind="stop",
        ):
            provider_call_started = operation.state in {
                "uncertain",
                "accepted",
            } or (
                operation.state == "prepared"
                and (
                    operation.response.get("providerCallStarted") is True
                    or operation.updated_at > operation.created_at
                    or bool(operation.last_error)
                )
            )
            if provider_call_started:
                irreversible_stop_job_ids.update(operation.target_job_ids)

        nodes_by_job_id = {node.job_id: node for node in nodes}
        reopen_job_ids = {
            intent.job_id
            for intent in pending_drain_intents
            if intent.state == "active"
            and intent.job_id not in irreversible_stop_job_ids
            and (node := nodes_by_job_id.get(intent.job_id)) is not None
            and node.heartbeat is not None
            and node.heartbeat_fresh
        }
        if reopen_job_ids:
            counterfactual_sandbox_nodes = _nodes_with_drain_admission_reopened(
                sandbox_nodes,
                reopen_job_ids,
            )
            counterfactual_builder_nodes = _nodes_with_drain_admission_reopened(
                builder_nodes,
                reopen_job_ids,
            )
            decision = evaluate_scale(
                counterfactual_sandbox_nodes,
                sandbox_demand,
                effective_policy,
                live_signals=live_scale_signals,
                program_signals=program_scale_signals,
            )
            builder_decision = evaluate_builder_scale(
                counterfactual_builder_nodes,
                pending_builds=builder_pending,
                prepared_builders=builder_prepared,
                policy=effective_policy,
                max_builder_nodes=config.builder.max_nodes,
            )
    sandbox_create_intents = build_create_intents(
        config,
        decision,
        role="sandbox",
        seed_prefix=args.seed_prefix,
    )
    builder_create_intents = build_create_intents(
        config,
        builder_decision,
        role="builder",
        seed_prefix=args.seed_prefix,
    )
    if "sandbox" in blocked_create_roles:
        sandbox_create_intents = []
    if "builder" in blocked_create_roles:
        builder_create_intents = []
    create_intents = [*sandbox_create_intents, *builder_create_intents]
    requested_sandbox_stop_job_ids = decision.stops
    requested_builder_stop_job_ids = builder_decision.stops
    (
        sandbox_stop_job_ids_with_detachable_storage,
        blocked_storage_native_detach_stop_job_ids,
    ) = partition_storage_native_detachable_stop_job_ids(
        sandbox_nodes,
        requested_sandbox_stop_job_ids,
        route_reservations or {},
    )
    sandbox_stop_job_ids, blocked_sandbox_stop_job_ids = partition_safe_stop_job_ids(
        sandbox_nodes,
        sandbox_stop_job_ids_with_detachable_storage,
        deployment_id=config.deployment_id,
        ownership_label=NODE_LABEL,
    )
    builder_stop_job_ids, blocked_builder_stop_job_ids = partition_safe_stop_job_ids(
        builder_nodes,
        requested_builder_stop_job_ids,
        deployment_id=config.deployment_id,
        ownership_label=BUILDER_LABEL,
    )
    requested_lost_sandbox_job_ids = tuple(
        node.job_id
        for node in sandbox_nodes
        if not node.job.is_final and node.job_id in set(destructive_power_cycle_job_ids)
    )
    requested_lost_builder_job_ids = tuple(
        node.job_id
        for node in builder_nodes
        if not node.job.is_final and node.job_id in set(destructive_power_cycle_job_ids)
    )
    lost_sandbox_stop_job_ids, blocked_lost_sandbox_job_ids = (
        partition_safe_stop_job_ids(
            sandbox_nodes,
            requested_lost_sandbox_job_ids,
            deployment_id=config.deployment_id,
            ownership_label=NODE_LABEL,
        )
    )
    lost_builder_stop_job_ids, blocked_lost_builder_job_ids = (
        partition_safe_stop_job_ids(
            builder_nodes,
            requested_lost_builder_job_ids,
            deployment_id=config.deployment_id,
            ownership_label=BUILDER_LABEL,
        )
    )
    destructive_stop_job_ids = tuple(
        dict.fromkeys([*lost_sandbox_stop_job_ids, *lost_builder_stop_job_ids])
    )
    requested_stop_job_ids = tuple(
        dict.fromkeys(
            [
                *requested_sandbox_stop_job_ids,
                *requested_builder_stop_job_ids,
                *requested_lost_sandbox_job_ids,
                *requested_lost_builder_job_ids,
            ]
        )
    )
    sandbox_stop_job_ids = tuple(
        dict.fromkeys([*sandbox_stop_job_ids, *lost_sandbox_stop_job_ids])
    )
    builder_stop_job_ids = tuple(
        dict.fromkeys([*builder_stop_job_ids, *lost_builder_stop_job_ids])
    )
    stop_job_ids = tuple(dict.fromkeys([*sandbox_stop_job_ids, *builder_stop_job_ids]))
    blocked_stop_job_ids = tuple(
        dict.fromkeys(
            [
                *blocked_sandbox_stop_job_ids,
                *blocked_storage_native_detach_stop_job_ids,
                *blocked_builder_stop_job_ids,
                *blocked_lost_sandbox_job_ids,
                *blocked_lost_builder_job_ids,
            ]
        )
    )
    if drain_workflow_enabled:
        canceling_job_ids = {
            intent.job_id
            for intent in pending_drain_intents
            if intent.state == "canceling"
        }
        if canceling_job_ids:
            blocked_canceling = tuple(
                job_id
                for job_id in stop_job_ids
                if job_id in canceling_job_ids
                and job_id not in destructive_stop_job_ids
            )
            sandbox_stop_job_ids = tuple(
                job_id
                for job_id in sandbox_stop_job_ids
                if job_id not in canceling_job_ids or job_id in destructive_stop_job_ids
            )
            builder_stop_job_ids = tuple(
                job_id
                for job_id in builder_stop_job_ids
                if job_id not in canceling_job_ids or job_id in destructive_stop_job_ids
            )
            stop_job_ids = (*sandbox_stop_job_ids, *builder_stop_job_ids)
            blocked_stop_job_ids = (*blocked_stop_job_ids, *blocked_canceling)
    stop_nodes_by_job_id = {
        node.job_id: node for node in (*sandbox_nodes, *builder_nodes)
    }
    unreachable_stop_job_ids = tuple(
        job_id
        for job_id in stop_job_ids
        if (node := stop_nodes_by_job_id.get(job_id)) is not None
        and unreachable_node_stop_ready(node, effective_policy)
    )
    unreachable_stop_job_id_set = set(unreachable_stop_job_ids)
    active_drain_intents: list[DrainIntent] = []
    drain_results: list[dict[str, Any]] = []
    storage_native_migration_results: list[dict[str, Any]] = []
    storage_native_detach_results: list[dict[str, Any]] = []
    pending_delete_results: list[dict[str, Any]] = []
    drain_ready_stop_job_ids: list[str] = []
    canceled_drain_job_ids: list[str] = []
    remaining_storage_cleanup_budget = max(
        0,
        config.autoscaler_max_storage_native_detaches_per_cycle,
    )
    detach_gateway_url = f"http://127.0.0.1:{config.gateway_port}"
    detach_gateway_error = ""
    node_control_bearer_token = read_required_token_file(
        config.node_control_token_file(),
        "node control bearer token",
    )
    gateway_control_bearer_token = read_required_token_file(
        config.gateway_token_file(),
        "gateway control bearer token",
    )
    if execution_requested and execution_authorized:
        pending_delete_routes = sorted(
            (route for route in sandbox_routes if route.delete_operation_id),
            key=lambda route: (route.updated_at, route.sandbox_id),
        )
        for route in pending_delete_routes[:remaining_storage_cleanup_budget]:
            delete_error = ""
            delete_payload: dict[str, Any] = {}
            try:
                delete_payload = _delete_gateway_sandbox(
                    detach_gateway_url,
                    route.sandbox_id,
                    bearer_token=gateway_control_bearer_token,
                )
            except Exception as exc:
                # The gateway and node reuse the route's durable delete
                # operation id, so an ambiguous replay is safe next cycle.
                delete_error = str(exc)
            pending_delete_results.append(
                {
                    "job_id": route.job_id,
                    "sandbox_id": route.sandbox_id,
                    "gateway_url": detach_gateway_url,
                    "delete_operation_id": route.delete_operation_id,
                    "request_succeeded": not delete_error,
                    "deleted": delete_payload.get("deleted"),
                    "error": delete_error,
                }
            )
            remaining_storage_cleanup_budget -= 1
    remaining_detach_budget = remaining_storage_cleanup_budget
    if drain_workflow_enabled:
        nodes_by_job_id = {node.job_id: node for node in nodes}
        for job_id in destructive_stop_job_ids:
            provider_state.retire_drain_intent(
                deployment_id=operation_deployment_id,
                job_id=job_id,
            )
        pending_drain_intents = provider_state.pending_drain_intents(
            deployment_id=operation_deployment_id,
        )
        desired_stop_job_ids = set(stop_job_ids)
        for intent in pending_drain_intents:
            node = nodes_by_job_id.get(intent.job_id)
            if (
                intent.state == "active"
                and intent.job_id not in desired_stop_job_ids
                and intent.job_id not in irreversible_stop_job_ids
                and node is not None
                and node.heartbeat is not None
                and node.heartbeat_fresh
            ):
                provider_state.begin_drain_cancellation(
                    deployment_id=intent.deployment_id,
                    job_id=intent.job_id,
                )

        sandbox_stop_set = set(sandbox_stop_job_ids)
        for job_id in stop_job_ids:
            if (
                job_id in unreachable_stop_job_id_set
                or job_id in destructive_stop_job_ids
            ):
                continue
            provider_state.prepare_drain_intent(
                deployment_id=operation_deployment_id,
                job_id=job_id,
                role="sandbox" if job_id in sandbox_stop_set else "builder",
            )

        pending_drain_intents = provider_state.pending_drain_intents(
            deployment_id=operation_deployment_id,
        )
        for intent in pending_drain_intents:
            node = nodes_by_job_id.get(intent.job_id)
            heartbeat = node.heartbeat if node is not None else None
            node_url = str(heartbeat.node_url or "").strip() if heartbeat else ""
            response: dict[str, Any] = {}
            error = ""
            if (
                intent.state == "active"
                and intent.job_id in unreachable_stop_job_id_set
            ):
                error = "unreachable stale-node stop proof selected"
            elif not node_url:
                error = "fresh node heartbeat has no node URL"
            else:
                try:
                    if intent.state == "canceling":
                        response = _post_node_drain(
                            node_url,
                            intent.token,
                            draining=False,
                            bearer_token=node_control_bearer_token,
                        )
                    elif node_control_bearer_token is None:
                        response = _post_node_drain(node_url, intent.token)
                    else:
                        response = _post_node_drain(
                            node_url,
                            intent.token,
                            bearer_token=node_control_bearer_token,
                        )
                except Exception as exc:
                    # A timeout or malformed response is ambiguous. The stable
                    # intent remains in its current direction and a canceling
                    # intent can never authorize a provider stop.
                    error = str(exc)
            drain_acknowledged = bool(
                intent.state == "active"
                and not error
                and _drain_response_acknowledges(
                    response,
                    token=intent.token,
                    draining=True,
                )
            )
            if (
                drain_acknowledged
                and intent.role == "sandbox"
                and heartbeat is not None
                and STORAGE_NATIVE_CAPABILITY in heartbeat.capabilities
                and STORAGE_NATIVE_DETACH_CAPABILITY in heartbeat.capabilities
                and remaining_detach_budget > 0
            ):
                parked_routes = [
                    route
                    for route in (route_reservations or {}).get(intent.job_id, ())
                    if is_worker_detachable_parked_route(route)
                ]
                for route in parked_routes[:remaining_detach_budget]:
                    detach_error = detach_gateway_error
                    detach_payload: dict[str, Any] = {}
                    if not detach_error and not detach_gateway_url:
                        detach_error = (
                            "gateway control URL is required for storage-native "
                            "worker detach"
                        )
                    if not detach_error:
                        try:
                            detach_payload = _post_gateway_sandbox_detach(
                                detach_gateway_url,
                                route.sandbox_id,
                                bearer_token=gateway_control_bearer_token,
                            )
                        except Exception as exc:
                            # The route and node deletion journals make an ambiguous
                            # request safe to retry in a later autoscaler cycle.
                            detach_error = str(exc)
                    storage_native_detach_results.append(
                        {
                            "job_id": intent.job_id,
                            "sandbox_id": route.sandbox_id,
                            "gateway_url": detach_gateway_url,
                            "request_succeeded": not detach_error,
                            "sandbox": detach_payload.get("sandbox", {}),
                            "error": detach_error,
                        }
                    )
                    remaining_detach_budget -= 1
                    if remaining_detach_budget <= 0:
                        break
            cancellation_acknowledged = False
            ready = False
            if intent.state == "canceling":
                cancellation_acknowledged = bool(
                    not error
                    and _drain_response_acknowledges(
                        response,
                        token=intent.token,
                        draining=False,
                    )
                )
                if cancellation_acknowledged:
                    provider_state.retire_drain_intent(
                        deployment_id=intent.deployment_id,
                        job_id=intent.job_id,
                    )
                    canceled_drain_job_ids.append(intent.job_id)
            else:
                ready = (
                    not error
                    and node is not None
                    and node_drain_ready(node, intent.token)
                )
                if ready:
                    drain_ready_stop_job_ids.append(intent.job_id)
            drain_results.append(
                {
                    "jobId": intent.job_id,
                    "role": intent.role,
                    "action": ("undrain" if intent.state == "canceling" else "drain"),
                    "nodeUrl": node_url,
                    "requestSucceeded": not error,
                    "heartbeatReady": ready,
                    "cancellationAcknowledged": cancellation_acknowledged,
                    "error": error,
                }
            )
        pending_drain_intents = provider_state.pending_drain_intents(
            deployment_id=operation_deployment_id,
        )
        active_drain_intents = [
            intent for intent in pending_drain_intents if intent.state == "active"
        ]
    active_drain_job_ids = {intent.job_id for intent in active_drain_intents}
    canceling_drain_job_ids = {
        intent.job_id for intent in pending_drain_intents if intent.state == "canceling"
    }
    pending_drain_job_ids = active_drain_job_ids | canceling_drain_job_ids
    active_bootstrap_job_ids = {
        node.job_id
        for node in (*sandbox_nodes, *builder_nodes)
        if not node.job.is_final
    }
    bootstrap_records = prune_bootstrap_records(
        control_state.load_bootstrap_records(),
        active_bootstrap_job_ids,
    )
    completed_bootstrap_results: list[dict[str, Any]] = []
    if bootstrap_coordinator is not None and execution_authorized:
        bootstrap_records, completed_bootstrap_results = (
            bootstrap_coordinator.collect_completed(
                bootstrap_records,
                control_state,
                active_job_ids=active_bootstrap_job_ids,
            )
        )

    def refresh_access_for_instance(
        instance: ProviderInstance,
    ) -> InstanceBootstrapAccess:
        try:
            refreshed = provider.retrieve_instance(instance.id, include_updates=True)
        except ProviderError as exc:
            current = provider.bootstrap_access(instance)
            return replace(
                current,
                runnable=False,
                reason=f"Provider access refresh failed: {exc}",
            )
        return provider.bootstrap_access(refreshed)

    bootstrap_nodes = [*sandbox_nodes, *builder_nodes]
    max_bootstraps = config.autoscaler_max_init_per_cycle
    if bootstrap_coordinator is not None:
        in_flight_job_ids = bootstrap_coordinator.in_flight_job_ids
        bootstrap_nodes = [
            node for node in bootstrap_nodes if node.job_id not in in_flight_job_ids
        ]
        max_bootstraps = min(
            max_bootstraps,
            bootstrap_coordinator.available_slots,
        )
    bootstrap_intents = build_vm_bootstrap_intents(
        bootstrap_nodes,
        bootstrap_records,
        retry_seconds=config.autoscaler_init_retry_seconds,
        max_per_cycle=max_bootstraps,
        options_for_node=lambda node, role: vm_init_options_for_autoscaled_node(
            node,
            role,
            args,
            config,
        ),
        access_for_instance=provider.bootstrap_access,
        refresh_access_for_instance=(
            refresh_access_for_instance
            if args.execute and not getattr(args, "jobs_file", None)
            else None
        ),
        max_access_refreshes=max_bootstraps,
        excluded_job_ids=set(stop_job_ids) | pending_drain_job_ids,
    )
    refreshed_access = False
    for intent in bootstrap_intents:
        if intent.access_refreshed_at is not None:
            refreshed_access = True
            bootstrap_records = mark_bootstrap_access_refresh(
                bootstrap_records,
                intent,
            )
    if refreshed_access:
        control_state.save_bootstrap_records(bootstrap_records)
    bootstrap_intents = [
        apply_bootstrap_requirements(intent) for intent in bootstrap_intents
    ]
    journaled_create_operations: list[ProviderOperation] = []
    journaled_stop_operations: list[ProviderOperation] = []
    if execution_authorized and provider_state is not None:
        if args.execute:
            labeled_sandbox_intents: list[InstanceCreateIntent] = []
            labeled_builder_intents: list[InstanceCreateIntent] = []
            for role, intents, destination in (
                ("sandbox", sandbox_create_intents, labeled_sandbox_intents),
                ("builder", builder_create_intents, labeled_builder_intents),
            ):
                for intent in intents:
                    intent_key = provider_state.allocate_operation_intent_key(
                        deployment_id=operation_deployment_id,
                        kind="create",
                        base_key=f"{role}:{intent.seed}",
                    )
                    operation_id = stable_provider_operation_id(
                        operation_deployment_id,
                        "create",
                        intent_key,
                    )
                    labeled = with_provider_operation_label(
                        intent,
                        operation_id,
                        deployment_id=operation_deployment_id,
                    )
                    operation = provider_state.prepare_operation(
                        intent_key=intent_key,
                        kind="create",
                        deployment_id=operation_deployment_id,
                        role=role,
                        request=provider.render_create_request([labeled]),
                    )
                    journaled_create_operations.append(operation)
                    destination.append(labeled)
            sandbox_create_intents = labeled_sandbox_intents
            builder_create_intents = labeled_builder_intents
            create_intents = [*sandbox_create_intents, *builder_create_intents]
        if args.execute:
            stop_ids_to_journal = tuple(
                dict.fromkeys(
                    [
                        *drain_ready_stop_job_ids,
                        *unreachable_stop_job_ids,
                        *destructive_stop_job_ids,
                    ]
                )
            )
            sandbox_stop_set = set(sandbox_stop_job_ids)
            drain_intents_by_job = {
                intent.job_id: intent for intent in active_drain_intents
            }
            for job_id in stop_ids_to_journal:
                unreachable_ready = job_id in unreachable_stop_job_id_set
                destructively_lost = job_id in destructive_stop_job_ids
                drain_intent = drain_intents_by_job.get(job_id)
                role = (
                    drain_intent.role
                    if drain_intent is not None
                    and not unreachable_ready
                    and not destructively_lost
                    else ("sandbox" if job_id in sandbox_stop_set else "builder")
                )
                request: dict[str, Any] = {
                    "type": "bulk",
                    "items": [{"id": job_id}],
                }
                if unreachable_ready:
                    node = stop_nodes_by_job_id[job_id]
                    reference = unreachable_node_reference(node)
                    request.update(
                        {
                            "unreachableStaleReady": True,
                            "unreachableReference": (
                                reference.isoformat() if reference is not None else ""
                            ),
                            "routeCount": len(
                                (route_reservations or {}).get(job_id, ())
                            ),
                            "lastKnownActiveSandboxes": node.active_sandboxes,
                            "lastHeartbeatSafeToStop": True,
                            "lastHeartbeatPresent": node.heartbeat is not None,
                        }
                    )
                elif destructively_lost:
                    node = stop_nodes_by_job_id[job_id]
                    request.update(
                        {
                            "destructivePowerCycle": True,
                            "postStartSuspensionObserved": True,
                            "routeCount": len(
                                (route_reservations or {}).get(job_id, ())
                            ),
                            "lastKnownActiveSandboxes": node.active_sandboxes,
                        }
                    )
                elif drain_intent is None:
                    raise AutoscalerStateError(
                        f"drain-ready job has no durable intent: {job_id}"
                    )
                else:
                    request.update(
                        {
                            "drainToken": drain_intent.token,
                            "drainReady": True,
                        }
                    )
                intent_key = (
                    f"{role}:{job_id}:unreachable:{request['unreachableReference']}"
                    if unreachable_ready
                    else (
                        f"{role}:{job_id}:destructive-power-cycle"
                        if destructively_lost
                        else f"{role}:{job_id}:{drain_intent.token}"
                    )
                )
                journaled_stop_operations.append(
                    provider_state.prepare_operation(
                        intent_key=intent_key,
                        kind="stop",
                        deployment_id=operation_deployment_id,
                        role=role,
                        request=request,
                        target_job_ids=(job_id,),
                    )
                )
    result: dict[str, Any] = {
        "provider": {
            "kind": provider.kind,
            "scopeId": provider.scope_id,
        },
        "controlStateFile": str(control_state_file),
        "nodes": [node_to_dict(node) for node in nodes],
        "decision": scale_decision_to_dict(decision),
        "effectivePolicy": dashboard_scale_policy_to_dict(effective_policy),
        "programWakePlan": program_wake_plan,
        "builderDecision": scale_decision_to_dict(builder_decision),
        "pendingImageBuilds": builder_pending,
        "activeImageBuilds": active_image_builds,
        "preparedBuilderCount": builder_prepared,
        "unexpectedly_suspended_job_ids": sorted(
            node.job_id for node in (*sandbox_nodes, *builder_nodes) if node.job.is_lost
        ),
        "destructive_power_cycle_job_ids": list(destructive_power_cycle_job_ids),
        "lost_sandbox_ids": [route.sandbox_id for route in lost_sandbox_routes],
        "buildWarmSandboxResources": build_warm_resources.to_dict(),
        "createIntents": [intent.to_dict() for intent in create_intents],
        "requestedStopJobIds": list(requested_stop_job_ids),
        "stopJobIds": list(stop_job_ids),
        "blockedStopJobIds": list(blocked_stop_job_ids),
        "blocked_storage_native_detach_stop_job_ids": list(
            blocked_storage_native_detach_stop_job_ids
        ),
        "drainingJobIds": sorted(active_drain_job_ids),
        "cancelingDrainJobIds": sorted(canceling_drain_job_ids),
        "canceledDrainJobIds": sorted(canceled_drain_job_ids),
        "drainReadyStopJobIds": list(drain_ready_stop_job_ids),
        "unreachableReadyStopJobIds": list(unreachable_stop_job_ids),
        "destructive_stop_job_ids": list(destructive_stop_job_ids),
        "drainIntents": [
            _drain_intent_to_dict(intent) for intent in pending_drain_intents
        ],
        "drainResults": drain_results,
        "pending_delete_results": pending_delete_results,
        "storage_native_migration_results": storage_native_migration_results,
        "storage_native_detach_results": storage_native_detach_results,
        "prunedFinalHeartbeats": list(final_heartbeat_job_ids),
        "prunedOrphanedStaleHeartbeats": list(orphaned_stale_heartbeat_job_ids),
        "fencedPowerCycleHeartbeats": sorted(
            set(fenced_heartbeat_job_ids) - set(final_heartbeat_job_ids)
        ),
        "removedStoppedHeartbeats": [],
        "bootstrapIntents": [
            vm_bootstrap_intent_to_dict(intent) for intent in bootstrap_intents
        ],
        "bootstrapResults": list(completed_bootstrap_results),
        "execute": bool(args.execute and execution_authorized),
        "controllerLockHeld": provider_mutations_allowed,
        "blockedCreateRoles": sorted(blocked_create_roles),
        "createVisibilityGuards": create_visibility_guards,
        "providerOperationResults": [],
        "sandboxCapacityOperationSucceeded": False,
        "builderCapacityOperationSucceeded": False,
        "definitelyTerminatedJobIds": [],
        "rawNodes": nodes,
        "rawSandboxNodes": sandbox_nodes,
        "rawBuilderNodes": builder_nodes,
        "rawDecision": decision,
        "rawBuilderDecision": builder_decision,
        "rawCreateIntents": create_intents,
        "rawBootstrapIntents": bootstrap_intents,
    }

    if (
        execution_authorized
        and provider_state is not None
        and (
            journaled_create_operations
            or journaled_stop_operations
            or any(
                operation.kind == "stop"
                for operation in provider_state.list_operations(states={"prepared"})
            )
        )
    ):
        planned_allowed_kinds: set[str] = set()
        if args.execute:
            planned_allowed_kinds.update(("create", "stop"))
        planned_results = apply_prepared_provider_operations(
            provider_state,
            provider,
            source="planned",
            allowed_kinds=planned_allowed_kinds,
            allowed_stop_operation_ids={
                operation.operation_id for operation in journaled_stop_operations
            },
            telemetry=telemetry,
        )
        provider_operation_results.extend(planned_results)
        # An already-applied stop can be encountered again before the next job
        # inventory observes it final; it remains definite and is never replayed.
        for operation in journaled_stop_operations:
            current = provider_state.get_operation(operation.operation_id)
            if (
                current is not None
                and current.state == "accepted"
                and not any(
                    item.operation_id == current.operation_id
                    for item in provider_operation_results
                )
            ):
                provider_operation_results.append(
                    ProviderOperationOutcome.from_operation(
                        current,
                        source="journal",
                    )
                )
        result["createdJobIds"] = [
            job_id
            for item in planned_results
            if item.kind == "create" and item.state == "accepted"
            for job_id in item.job_ids
        ]
    definitely_terminated = sorted(
        {
            job_id
            for item in provider_operation_results
            if item.kind == "stop" and item.state in {"accepted", "recovered"}
            for job_id in item.job_ids
        }
    )
    result["definitelyTerminatedJobIds"] = definitely_terminated
    if definitely_terminated:
        result["removedStoppedHeartbeats"] = sorted(
            control_state.remove_heartbeats(definitely_terminated)
        )
    result["sandboxCapacityOperationSucceeded"] = _sandbox_capacity_operation_succeeded(
        provider_operation_results,
        decision.resource_deficit,
        effective_policy.default_node_resources,
    )
    desired_builders = min(
        max(1 if builder_pending > 0 else 0, builder_prepared),
        config.builder.max_nodes,
    )
    result["builderCapacityOperationSucceeded"] = _builder_capacity_operation_succeeded(
        provider_operation_results,
        existing_builders=builder_decision.total_nodes,
        desired_builders=desired_builders,
    )

    if bootstrap_coordinator is not None and args.execute and execution_authorized:
        bootstrap_results = list(completed_bootstrap_results)
        for intent in bootstrap_intents:
            if not intent.runnable or not intent.access.command:
                bootstrap_results.append(
                    {
                        "jobId": intent.job_id,
                        "nodeId": intent.node_id,
                        "role": intent.role,
                        "skipped": True,
                        "reason": intent.reason,
                    }
                )
                continue
            bootstrap_records, scheduled = bootstrap_coordinator.submit(
                intent,
                config,
                bootstrap_records,
                control_state,
                assert_provider_fence=assert_provider_fence,
            )
            bootstrap_results.append(scheduled)
        result["bootstrapResults"] = bootstrap_results
        control_state.save_bootstrap_records(bootstrap_records)
    elif args.execute and execution_authorized and bootstrap_intents:
        ordered_bootstrap_results: list[dict[str, Any] | None] = [None] * len(
            bootstrap_intents
        )
        runnable_bootstraps: list[
            tuple[int, VmBootstrapIntent, int, datetime, float]
        ] = []
        for index, intent in enumerate(bootstrap_intents):
            if not intent.runnable or not intent.access.command:
                ordered_bootstrap_results[index] = {
                    "jobId": intent.job_id,
                    "nodeId": intent.node_id,
                    "role": intent.role,
                    "skipped": True,
                    "reason": intent.reason,
                }
                continue
            assert_provider_fence()
            attempt_started_at = utc_now()
            attempt_started_perf = time.perf_counter()
            bootstrap_records = mark_bootstrap_attempt(bootstrap_records, intent)
            control_state.save_bootstrap_records(bootstrap_records)
            attempt_record = bootstrap_records.get(intent.job_id)
            attempt_count = (
                attempt_record.attempts
                if attempt_record is not None
                else intent.previous_attempts + 1
            )
            runnable_bootstraps.append(
                (
                    index,
                    intent,
                    attempt_count,
                    attempt_started_at,
                    attempt_started_perf,
                )
            )

        if runnable_bootstraps:
            max_workers = min(
                len(runnable_bootstraps),
                max(1, config.autoscaler_max_init_per_cycle),
            )
            futures: dict[
                Future[_VmBootstrapAttemptResult],
                tuple[int, VmBootstrapIntent, int, datetime, float],
            ] = {}
            with ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="vm-bootstrap",
            ) as executor:
                for prepared in runnable_bootstraps:
                    index, intent, attempt_count, _started_at, started_perf = prepared
                    try:
                        assert_provider_fence()
                        future = executor.submit(
                            _execute_vm_bootstrap_attempt,
                            intent,
                            config,
                            attempt_count=attempt_count,
                            assert_provider_fence=assert_provider_fence,
                            attempt_started_perf=started_perf,
                            telemetry=telemetry,
                            trace_context=telemetry.current_trace_headers(),
                        )
                    except Exception as exc:
                        future = Future()
                        future.set_result(
                            _failed_vm_bootstrap_attempt(
                                intent,
                                started_perf,
                                str(exc),
                            )
                        )
                    futures[future] = prepared

                for future in as_completed(futures):
                    index, intent, attempt_count, started_at, started_perf = futures[
                        future
                    ]
                    try:
                        attempt_result = future.result()
                    except Exception as exc:
                        attempt_result = _failed_vm_bootstrap_attempt(
                            intent,
                            started_perf,
                            str(exc),
                        )

                    if attempt_result.status == "succeeded":
                        bootstrap_records = mark_bootstrap_success(
                            bootstrap_records,
                            intent,
                        )
                    else:
                        bootstrap_records = mark_bootstrap_failure(
                            bootstrap_records,
                            intent,
                            attempt_result.error,
                            retry_delay_seconds=attempt_result.retry_delay_seconds,
                        )
                    # Only the controller thread mutates and persists the aggregate
                    # state, once for each independently completed remote attempt.
                    control_state.save_bootstrap_records(bootstrap_records)
                    ordered_bootstrap_results[index] = attempt_result.result
                    record_vm_init_attempt_result(
                        metrics_store,
                        intent,
                        status=attempt_result.status,
                        attempts=attempt_count,
                        started_at=started_at,
                        attempt_started_perf=started_perf,
                        stage_duration_ms=attempt_result.stage_duration_ms,
                        run_duration_ms=attempt_result.run_duration_ms,
                        returncode=attempt_result.returncode,
                        error=attempt_result.error,
                        retry_delay_seconds=attempt_result.retry_delay_seconds,
                        init_phases_ms=attempt_result.init_phases_ms,
                        init_total_ms=attempt_result.init_total_ms,
                    )

        bootstrap_results: list[dict[str, Any]] = []
        for index, item in enumerate(ordered_bootstrap_results):
            if item is None:
                intent = bootstrap_intents[index]
                error = "VM init attempt did not produce a result"
                bootstrap_records = mark_bootstrap_failure(
                    bootstrap_records,
                    intent,
                    error,
                )
                control_state.save_bootstrap_records(bootstrap_records)
                item = {
                    "jobId": intent.job_id,
                    "nodeId": intent.node_id,
                    "role": intent.role,
                    "returncode": None,
                    "status": "failed",
                    "error": error,
                    "durationMs": 0,
                }
            bootstrap_results.append(item)
        result["bootstrapResults"] = bootstrap_results
    elif args.execute and execution_authorized:
        control_state.save_bootstrap_records(bootstrap_records)
    if execution_authorized and provider_state is not None:
        result["compactedProviderOperations"] = provider_state.compact_terminal_history(
            keep=1000
        )
    result["providerOperationResults"] = [
        item.to_dict() for item in provider_operation_results
    ]
    return result


def record_submitted_vm_metrics(
    metrics_store: MetricsStore,
    cycle: int,
    result: dict[str, Any],
) -> None:
    job_ids = list(result.get("createdJobIds") or [])
    intents = list(result.get("rawCreateIntents") or [])
    for job_id, intent in zip(job_ids, intents):
        record_vm_submitted(
            metrics_store, cycle=cycle, job_id=str(job_id), intent=intent
        )


def record_observed_vm_metrics(
    metrics_store: MetricsStore,
    cycle: int,
    result: dict[str, Any],
    observed_vm_keys: dict[str, tuple[object, ...]],
) -> None:
    nodes = [
        *list(result.get("rawSandboxNodes") or []),
        *list(result.get("rawBuilderNodes") or []),
    ]
    for node in nodes:
        job = getattr(node, "job", None)
        if job is None or not getattr(job, "id", ""):
            continue
        job_id = str(job.id)
        if getattr(job, "is_final", False) and job_id not in observed_vm_keys:
            continue
        key = (
            getattr(job, "state", ""),
            getattr(job, "started_at", None),
            getattr(job, "expires_at", None),
            getattr(job, "latest_note", None),
            bool(getattr(node, "heartbeat_fresh", False)),
            bool(getattr(node, "is_ready", False)),
        )
        if observed_vm_keys.get(job_id) == key:
            continue
        observed_vm_keys[job_id] = key
        record_vm_observed(metrics_store, cycle=cycle, node=node)


@dataclass(frozen=True)
class _VmBootstrapAttemptResult:
    result: dict[str, Any]
    status: str
    returncode: int | None
    error: str = ""
    stage_duration_ms: int | None = None
    run_duration_ms: int | None = None
    retry_delay_seconds: int | None = None
    init_phases_ms: dict[str, int] | None = None
    init_total_ms: int | None = None


@dataclass(frozen=True)
class _InFlightVmBootstrap:
    intent: VmBootstrapIntent
    attempt_count: int
    started_at: datetime
    started_perf: float
    future: Future[_VmBootstrapAttemptResult]


class _VmBootstrapCoordinator:
    """Run VM init attempts across reconcile cycles without worker state writes."""

    def __init__(
        self,
        max_workers: int,
        metrics_store: MetricsStore | None,
        *,
        telemetry: Telemetry | None = None,
    ) -> None:
        if max_workers < 1:
            raise ValueError("VM bootstrap concurrency must be positive")
        self.max_workers = max_workers
        self.metrics_store = metrics_store
        self.telemetry = telemetry or _DISABLED_AUTOSCALER_TELEMETRY
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="vm-bootstrap",
        )
        self._in_flight: dict[str, _InFlightVmBootstrap] = {}
        self._activity = Event()
        self._next_retry_deadline: float | None = None

    @property
    def in_flight_job_ids(self) -> frozenset[str]:
        return frozenset(self._in_flight)

    @property
    def available_slots(self) -> int:
        return max(0, self.max_workers - len(self._in_flight))

    def submit(
        self,
        intent: VmBootstrapIntent,
        config: DeploymentConfig,
        records: dict[str, VmBootstrapRecord],
        store: ControlStateStore,
        *,
        assert_provider_fence: Callable[[], None],
    ) -> tuple[dict[str, VmBootstrapRecord], dict[str, Any]]:
        if intent.job_id in self._in_flight:
            raise AutoscalerStateError(
                f"VM init is already in flight for {intent.job_id}"
            )
        if self.available_slots <= 0:
            raise AutoscalerStateError("VM init concurrency is exhausted")
        assert_provider_fence()
        started_at = utc_now()
        started_perf = time.perf_counter()
        records = mark_bootstrap_attempt(records, intent, now=started_at)
        store.save_bootstrap_records(records)
        record = records.get(intent.job_id)
        attempt_count = (
            record.attempts if record is not None else intent.previous_attempts + 1
        )
        try:
            trace_context = self.telemetry.current_trace_headers()
            future = self._executor.submit(
                _execute_vm_bootstrap_attempt,
                intent,
                config,
                attempt_count=attempt_count,
                assert_provider_fence=assert_provider_fence,
                attempt_started_perf=started_perf,
                telemetry=self.telemetry,
                trace_context=trace_context,
            )
            future.add_done_callback(lambda _future: self._activity.set())
        except Exception as exc:
            future = Future()
            future.set_result(
                _failed_vm_bootstrap_attempt(intent, started_perf, str(exc))
            )
        self._in_flight[intent.job_id] = _InFlightVmBootstrap(
            intent=intent,
            attempt_count=attempt_count,
            started_at=started_at,
            started_perf=started_perf,
            future=future,
        )
        return records, {
            "jobId": intent.job_id,
            "nodeId": intent.node_id,
            "role": intent.role,
            "status": "attempting",
            "attempts": attempt_count,
        }

    def collect_completed(
        self,
        records: dict[str, VmBootstrapRecord],
        store: ControlStateStore,
        *,
        active_job_ids: set[str],
    ) -> tuple[dict[str, VmBootstrapRecord], list[dict[str, Any]]]:
        results: list[dict[str, Any]] = []
        for job_id, prepared in tuple(self._in_flight.items()):
            if not prepared.future.done():
                continue
            del self._in_flight[job_id]
            try:
                attempt_result = prepared.future.result()
            except Exception as exc:
                attempt_result = _failed_vm_bootstrap_attempt(
                    prepared.intent,
                    prepared.started_perf,
                    str(exc),
                )
            if job_id in active_job_ids:
                if attempt_result.status == "succeeded":
                    records = mark_bootstrap_success(records, prepared.intent)
                else:
                    records = mark_bootstrap_failure(
                        records,
                        prepared.intent,
                        attempt_result.error,
                        retry_delay_seconds=attempt_result.retry_delay_seconds,
                    )
                    if attempt_result.retry_delay_seconds is not None:
                        retry_deadline = time.monotonic() + max(
                            0,
                            attempt_result.retry_delay_seconds,
                        )
                        self._next_retry_deadline = (
                            retry_deadline
                            if self._next_retry_deadline is None
                            else min(self._next_retry_deadline, retry_deadline)
                        )
                store.save_bootstrap_records(records)
            duration_ms = _bootstrap_result_duration_ms(attempt_result)
            record_vm_init_attempt_result(
                self.metrics_store,
                prepared.intent,
                status=attempt_result.status,
                attempts=prepared.attempt_count,
                started_at=prepared.started_at,
                attempt_started_perf=prepared.started_perf,
                stage_duration_ms=attempt_result.stage_duration_ms,
                run_duration_ms=attempt_result.run_duration_ms,
                returncode=attempt_result.returncode,
                error=attempt_result.error,
                retry_delay_seconds=attempt_result.retry_delay_seconds,
                init_phases_ms=attempt_result.init_phases_ms,
                init_total_ms=attempt_result.init_total_ms,
                duration_ms=duration_ms,
                finished_at=(prepared.started_at + timedelta(milliseconds=duration_ms)),
            )
            results.append(attempt_result.result)
        return records, results

    def wait_for_activity(self, timeout_seconds: float) -> None:
        timeout = max(0.0, timeout_seconds)
        retry_deadline = self._next_retry_deadline
        if retry_deadline is not None:
            retry_remaining = retry_deadline - time.monotonic()
            if retry_remaining <= 0:
                self._next_retry_deadline = None
                return
            timeout = min(timeout, retry_remaining)
        # Clear before checking futures so a completion cannot be lost between
        # the predicate and the blocking wait.
        self._activity.clear()
        if any(item.future.done() for item in self._in_flight.values()):
            return
        self._activity.wait(timeout)
        if (
            self._next_retry_deadline is not None
            and self._next_retry_deadline <= time.monotonic()
        ):
            self._next_retry_deadline = None

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=not wait)


def _execute_vm_bootstrap_attempt(
    intent: VmBootstrapIntent,
    config: DeploymentConfig,
    *,
    attempt_count: int,
    assert_provider_fence: Callable[[], None],
    attempt_started_perf: float,
    telemetry: Telemetry | None = None,
    trace_context: dict[str, str] | None = None,
) -> _VmBootstrapAttemptResult:
    telemetry = telemetry or _DISABLED_AUTOSCALER_TELEMETRY
    with telemetry.span(
        "vm.bootstrap",
        attributes={
            "cloud.instance.id": intent.job_id,
            "service.instance.id": intent.node_id,
            "vm.role": intent.role,
            "vm.bootstrap.attempt": attempt_count,
        },
        parent_context=telemetry.extracted_context(trace_context or {}),
    ) as span:
        result = _execute_vm_bootstrap_attempt_unobserved(
            intent,
            config,
            attempt_count=attempt_count,
            assert_provider_fence=assert_provider_fence,
            attempt_started_perf=attempt_started_perf,
        )
        span.set_attribute("vm.bootstrap.status", result.status)
        if result.status != "succeeded":
            span.status = "error"
            if result.error:
                span.set_attribute("error.message", result.error)
        if result.stage_duration_ms is not None:
            span.set_attribute(
                "vm.bootstrap.stage_duration_ms", result.stage_duration_ms
            )
        if result.run_duration_ms is not None:
            span.set_attribute("vm.bootstrap.run_duration_ms", result.run_duration_ms)
        return result


def _execute_vm_bootstrap_attempt_unobserved(
    intent: VmBootstrapIntent,
    config: DeploymentConfig,
    *,
    attempt_count: int,
    assert_provider_fence: Callable[[], None],
    attempt_started_perf: float,
) -> _VmBootstrapAttemptResult:
    stage_duration_ms: int | None = None
    run_duration_ms: int | None = None
    stage_payload: dict[str, Any] | None = None
    try:
        assert_provider_fence()
        effective_options = intent.options
        known_hosts_file = _bootstrap_known_hosts_file(intent, config)
        stage_started_perf = time.perf_counter()
        stage_result = stage_vm_init_package_over_ssh(
            intent.access.command,
            intent.options,
            timeout_seconds=config.autoscaler_init_timeout_seconds,
            private_key_file=str(config.init_ssh_private_key_file()),
            known_hosts_file=known_hosts_file,
        )
        stage_elapsed_ms = int((time.perf_counter() - stage_started_perf) * 1000)
        stage_duration_ms = stage_elapsed_ms
        stage_payload = {
            "localPath": str(stage_result.local_path),
            "remotePath": stage_result.remote_path,
            "command": list(stage_result.command),
            "returncode": stage_result.returncode,
            "durationMs": stage_duration_ms,
            "reused": stage_result.reused,
        }
        if stage_result.returncode != 0:
            error = f"package staging exited with status {stage_result.returncode}"
            retry_delay_seconds = _bootstrap_retry_delay_seconds(
                stage_result.returncode,
                attempt_count=attempt_count,
                configured_retry_seconds=config.autoscaler_init_retry_seconds,
            )
            return _VmBootstrapAttemptResult(
                result={
                    "jobId": intent.job_id,
                    "nodeId": intent.node_id,
                    "role": intent.role,
                    "returncode": stage_result.returncode,
                    "status": "failed",
                    "error": error,
                    "packageStage": stage_payload,
                    "durationMs": _elapsed_ms(attempt_started_perf),
                    "retryDelaySeconds": retry_delay_seconds,
                },
                status="failed",
                returncode=stage_result.returncode,
                error=error,
                stage_duration_ms=stage_duration_ms,
                retry_delay_seconds=retry_delay_seconds,
            )
        effective_options = replace(
            intent.options,
            package_spec=stage_result.remote_path,
            package_sha256=stage_result.package_sha256,
        )

        assert_provider_fence()
        run_started_perf = time.perf_counter()
        run_result = run_init_over_ssh(
            intent.access.command,
            render_vm_init_script(effective_options),
            timeout_seconds=config.autoscaler_init_timeout_seconds,
            private_key_file=str(config.init_ssh_private_key_file()),
            known_hosts_file=known_hosts_file,
        )
        run_duration_ms = int((time.perf_counter() - run_started_perf) * 1000)
        init_phases_ms = dict(getattr(run_result, "phase_durations_ms", ()))
        init_total_ms = getattr(run_result, "total_duration_ms", None)
        if run_result.returncode == 0:
            return _VmBootstrapAttemptResult(
                result={
                    "jobId": intent.job_id,
                    "nodeId": intent.node_id,
                    "role": intent.role,
                    "returncode": 0,
                    "status": "succeeded",
                    "packageStage": stage_payload,
                    "durationMs": _elapsed_ms(attempt_started_perf),
                    "runDurationMs": run_duration_ms,
                    "initPhasesMs": init_phases_ms,
                    "initTotalMs": init_total_ms,
                },
                status="succeeded",
                returncode=0,
                stage_duration_ms=stage_duration_ms,
                run_duration_ms=run_duration_ms,
                init_phases_ms=init_phases_ms,
                init_total_ms=init_total_ms,
            )

        error = f"init command exited with status {run_result.returncode}"
        retry_delay_seconds = _bootstrap_retry_delay_seconds(
            run_result.returncode,
            attempt_count=attempt_count,
            configured_retry_seconds=config.autoscaler_init_retry_seconds,
        )
        return _VmBootstrapAttemptResult(
            result={
                "jobId": intent.job_id,
                "nodeId": intent.node_id,
                "role": intent.role,
                "returncode": run_result.returncode,
                "status": "failed",
                "error": error,
                "packageStage": stage_payload,
                "durationMs": _elapsed_ms(attempt_started_perf),
                "runDurationMs": run_duration_ms,
                "retryDelaySeconds": retry_delay_seconds,
                "initPhasesMs": init_phases_ms,
                "initTotalMs": init_total_ms,
            },
            status="failed",
            returncode=run_result.returncode,
            error=error,
            stage_duration_ms=stage_duration_ms,
            run_duration_ms=run_duration_ms,
            retry_delay_seconds=retry_delay_seconds,
            init_phases_ms=init_phases_ms,
            init_total_ms=init_total_ms,
        )
    except Exception as exc:
        return _failed_vm_bootstrap_attempt(
            intent,
            attempt_started_perf,
            str(exc),
            stage_duration_ms=stage_duration_ms,
            run_duration_ms=run_duration_ms,
        )


def _bootstrap_known_hosts_file(
    intent: VmBootstrapIntent,
    config: DeploymentConfig,
) -> str | None:
    directory = config.control_state_file().parent / "ssh-known-hosts"
    safe_job_id = "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in intent.job_id
    ).strip("_")
    if not safe_job_id:
        raise ValueError("job id cannot produce an SSH known-hosts filename")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    return str(directory / safe_job_id[:128])


def _failed_vm_bootstrap_attempt(
    intent: VmBootstrapIntent,
    attempt_started_perf: float,
    error: str,
    *,
    stage_duration_ms: int | None = None,
    run_duration_ms: int | None = None,
    retry_delay_seconds: int | None = None,
) -> _VmBootstrapAttemptResult:
    return _VmBootstrapAttemptResult(
        result={
            "jobId": intent.job_id,
            "nodeId": intent.node_id,
            "role": intent.role,
            "returncode": None,
            "status": "failed",
            "error": error,
            "durationMs": _elapsed_ms(attempt_started_perf),
        },
        status="failed",
        returncode=None,
        error=error,
        stage_duration_ms=stage_duration_ms,
        run_duration_ms=run_duration_ms,
        retry_delay_seconds=retry_delay_seconds,
    )


def _bootstrap_retry_delay_seconds(
    returncode: int | None,
    *,
    attempt_count: int,
    configured_retry_seconds: int,
) -> int | None:
    if returncode != 255:
        return None
    maximum = max(0, int(configured_retry_seconds))
    transient_delay = 1 << min(max(0, int(attempt_count) - 1), 5)
    return min(maximum, transient_delay)


def record_vm_init_attempt_result(
    metrics_store: MetricsStore | None,
    intent: VmBootstrapIntent,
    *,
    status: str,
    attempts: int,
    started_at: Any,
    attempt_started_perf: float,
    stage_duration_ms: int | None,
    run_duration_ms: int | None,
    returncode: int | None,
    error: str = "",
    retry_delay_seconds: int | None = None,
    init_phases_ms: dict[str, int] | None = None,
    init_total_ms: int | None = None,
    duration_ms: int | None = None,
    finished_at: datetime | None = None,
) -> None:
    effective_duration_ms = (
        _elapsed_ms(attempt_started_perf)
        if duration_ms is None
        else max(0, int(duration_ms))
    )
    effective_finished_at = finished_at or utc_now()
    record_vm_init_attempt(
        metrics_store,
        job_id=intent.job_id,
        node_id=intent.node_id,
        role=intent.role,
        status=status,
        attempts=attempts,
        started_at=started_at.isoformat(),
        finished_at=effective_finished_at.isoformat(),
        duration_ms=effective_duration_ms,
        stage_duration_ms=stage_duration_ms,
        run_duration_ms=run_duration_ms,
        returncode=returncode,
        error=error,
        retry_delay_seconds=retry_delay_seconds,
        init_phases_ms=init_phases_ms,
        init_total_ms=init_total_ms,
    )


def _elapsed_ms(started_perf: float) -> int:
    return max(0, int((time.perf_counter() - started_perf) * 1000))


def _bootstrap_result_duration_ms(result: _VmBootstrapAttemptResult) -> int:
    value = result.result.get("durationMs")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    return max(0, int(value))


def load_instances_for_plan(
    config: DeploymentConfig,
    provider: ComputeProvider,
    args: argparse.Namespace,
    *,
    telemetry: Telemetry | None = None,
) -> list[ProviderInstance]:
    if args.jobs_file:
        payload = json.loads(args.jobs_file.read_text(encoding="utf-8"))
        raw_items = payload.get("items") if isinstance(payload, dict) else payload
        if not isinstance(raw_items, list):
            raise ValueError(
                "Jobs payload must be a list or an object with an items list."
            )
        instances = [
            provider.decode_instance(item)
            for item in raw_items
            if isinstance(item, dict)
        ]
    else:
        with (telemetry or _DISABLED_AUTOSCALER_TELEMETRY).span(
            "provider.list_instances"
        ) as span:
            instances = provider.list_instances()
            span.set_attribute("provider.instance_count", len(instances))

    include_ids = {str(job_id) for job_id in args.include_job}
    jobs: list[ProviderInstance] = []
    for job in instances:
        if should_include_job(
            job,
            config,
            provider,
            include_ids,
        ):
            jobs.append(job)
    return jobs


def sandbox_route_reservations(
    routes: Iterable[SandboxRoute],
) -> dict[str, tuple[SandboxRoute, ...]]:
    routes_by_job: dict[str, list[SandboxRoute]] = {}
    for route in routes:
        if route.worker_state == "detached" and is_portable_parked_route(route):
            continue
        job_id = route.job_id.strip()
        if not job_id:
            continue
        routes_by_job.setdefault(job_id, []).append(route)
    return {
        job_id: tuple(sorted(items, key=lambda route: route.sandbox_id))
        for job_id, items in routes_by_job.items()
    }


def partition_storage_native_detachable_stop_job_ids(
    nodes: list[SandboxNode],
    requested_job_ids: tuple[str, ...],
    routes_by_job: dict[str, tuple[SandboxRoute, ...]],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Allow stops whose remaining routes can release worker-local storage."""
    if not requested_job_ids:
        return (), ()
    nodes_by_job_id = {node.job_id: node for node in nodes}
    allowed: list[str] = []
    blocked: list[str] = []
    for job_id in dict.fromkeys(requested_job_ids):
        if _storage_native_stop_set_has_detachable_storage(
            nodes,
            nodes_by_job_id,
            (job_id,),
            routes_by_job,
        ):
            allowed.append(job_id)
        else:
            blocked.append(job_id)
    return tuple(allowed), tuple(blocked)


def _storage_native_stop_set_has_detachable_storage(
    nodes: list[SandboxNode],
    nodes_by_job_id: dict[str, SandboxNode],
    stop_job_ids: tuple[str, ...],
    routes_by_job: dict[str, tuple[SandboxRoute, ...]],
) -> bool:
    del nodes
    for job_id in stop_job_ids:
        routes = tuple(
            route
            for route in routes_by_job.get(job_id, ())
            if route.worker_state != "detached"
        )
        if not routes:
            continue
        node = nodes_by_job_id.get(job_id)
        heartbeat = node.heartbeat if node is not None else None
        if not (
            node is not None
            and node.heartbeat_fresh
            and heartbeat is not None
            and heartbeat.inventory_complete
            and STORAGE_NATIVE_CAPABILITY in heartbeat.capabilities
            and STORAGE_NATIVE_DETACH_CAPABILITY in heartbeat.capabilities
            and all(is_worker_detachable_parked_route(route) for route in routes)
        ):
            return False
        for route in routes:
            inventory = {item.sandbox_id: item for item in heartbeat.inventory}
            if (
                route.worker_state == "attached"
                and not _heartbeat_inventory_contains_route(
                    inventory,
                    route,
                )
            ):
                return False
    return True


def apply_route_reservations_to_heartbeats(
    heartbeats: dict[str, NodeHeartbeat],
    routes_by_job: dict[str, tuple[SandboxRoute, ...]],
) -> dict[str, NodeHeartbeat]:
    """Conservatively merge newer route reservations into stale heartbeats."""

    reconciled: dict[str, NodeHeartbeat] = {}
    for job_id, heartbeat in heartbeats.items():
        inventory = {item.sandbox_id: item for item in heartbeat.inventory}
        missing_routes = [
            route
            for route in routes_by_job.get(job_id, ())
            if route.worker_state != "detached"
            if not _heartbeat_inventory_contains_route(inventory, route)
        ]
        missing_resources = ResourceQuantity()
        for route in missing_routes:
            missing_resources = missing_resources + ResourceQuantity(
                disk_mb=route.resources.disk_mb
            )
        used = heartbeat.used_resources + missing_resources
        active_sandboxes = heartbeat.active_sandboxes + len(missing_routes)
        reconciled[job_id] = replace(
            heartbeat,
            used_resources=used,
            active_sandboxes=active_sandboxes,
        )
    return reconciled


def apply_route_reservations_to_nodes(
    nodes: list[SandboxNode],
    routes_by_job: dict[str, tuple[SandboxRoute, ...]],
) -> list[SandboxNode]:
    """Keep route ownership visible even when a job has no heartbeat record."""

    reconciled: list[SandboxNode] = []
    for node in nodes:
        heartbeat = node.heartbeat
        owns_releasable_inventory = bool(
            node.heartbeat_fresh
            and heartbeat is not None
            and heartbeat.inventory_complete
            and STORAGE_NATIVE_CAPABILITY in heartbeat.capabilities
            and STORAGE_NATIVE_DETACH_CAPABILITY in heartbeat.capabilities
            and all(
                is_worker_detachable_parked_route(route)
                for route in routes_by_job.get(node.job_id, ())
                if route.worker_state != "detached"
            )
        )
        reconciled.append(
            replace(
                node,
                # Parked ownership is disk inventory, not active
                # compute. A fresh complete inventory can therefore enter the
                # storage-native detach workflow. Missing or stale observations
                # retain the conservative route-count fence.
                active_sandboxes=(
                    node.active_sandboxes
                    if owns_releasable_inventory
                    else max(
                        node.active_sandboxes,
                        len(routes_by_job.get(node.job_id, ())),
                    )
                ),
            )
        )
    return reconciled


def _heartbeat_inventory_contains_route(
    inventory: dict[str, Any],
    route: SandboxRoute,
) -> bool:
    item = inventory.get(route.sandbox_id)
    if item is None:
        return False
    if route.generation <= 0 and item.generation <= 0:
        return True
    return bool(
        route.generation == item.generation
        and route.create_operation_id == item.operation_id
        and route.spec_hash == item.spec_hash
    )


def build_activity_sandbox_warm_resources(
    *,
    active_image_builds: int,
    pending_image_builds: int,
    prepared_builder_count: int,
    policy: ScalePolicy,
) -> ResourceQuantity:
    del prepared_builder_count
    if max(0, active_image_builds) <= 0 and max(0, pending_image_builds) <= 0:
        return ResourceQuantity()
    # A build proves that a sandbox node will probably be needed soon, but it
    # says nothing about the eventual sandbox shape. Reserve a small runnable
    # probe to keep one node warm. Reserving the entire schedulable node shape
    # (especially all hard disk) demanded a pristine empty node and created a
    # second VM as soon as the first node stored even one image or sandbox.
    capacity = policy.default_node_resources
    return ResourceQuantity(
        vcpu=min(1.0, capacity.vcpu),
        memory_mb=min(512, capacity.memory_mb),
        disk_mb=min(1024, capacity.disk_mb),
    )


def demand_with_build_warm_resources(
    demand: SandboxDemand,
    build_warm_resources: ResourceQuantity,
) -> SandboxDemand:
    desired = demand.desired_resources
    supplement = ResourceQuantity(
        vcpu=max(0.0, build_warm_resources.vcpu - desired.vcpu),
        memory_mb=max(0, build_warm_resources.memory_mb - desired.memory_mb),
        disk_mb=max(0, build_warm_resources.disk_mb - desired.disk_mb),
    )
    if supplement == ResourceQuantity():
        return demand
    return replace(
        demand,
        prepared_placement_requests=(
            *demand.prepared_placement_requests,
            SandboxPlacementRequest(resources=supplement),
        ),
    )


def demand_with_lost_sandbox_replacement(
    demand: SandboxDemand,
    routes: Iterable[SandboxRoute],
) -> SandboxDemand:
    """Retain enough capacity to absorb client retries after destructive loss.

    This does not recreate the sandboxes.  Their process and writable state no
    longer exist.  It only prevents the autoscaler from interpreting route
    deletion as an idle fleet at exactly the moment replacement capacity is
    needed.  Dynamic admission keeps CPU/RAM at the largest individual shape
    while hard disk remains additive.
    """

    lost = tuple(route for route in routes if not is_portable_parked_route(route))
    if not lost:
        return demand
    resources = ResourceQuantity()
    requests: list[SandboxPlacementRequest] = []
    for route in lost:
        resources = resources + route.resources
        requests.append(
            SandboxPlacementRequest(
                resources=route.resources,
                excluded_job_ids=((route.job_id,) if route.job_id else ()),
            )
        )
    return replace(
        demand,
        pending_resources=demand.pending_resources + resources,
        pending_count=demand.pending_count + len(lost),
        placement_requests=(*demand.placement_requests, *requests),
    )


def should_include_job(
    job: ProviderInstance,
    config: DeploymentConfig,
    provider: ComputeProvider,
    include_ids: set[str],
) -> bool:
    if job.id in include_ids:
        return True
    if (
        not config.deployment_id
        or job.labels.get(DEPLOYMENT_LABEL) != config.deployment_id
    ):
        return False
    if not provider.instance_is_eligible(job):
        return False
    return (
        job.labels.get(NODE_LABEL) == "true" or job.labels.get(BUILDER_LABEL) == "true"
    )


def sandbox_pool_nodes(nodes: list[Any]) -> list[Any]:
    return [node for node in nodes if node.job.labels.get(NODE_LABEL) == "true"]


def builder_pool_nodes(nodes: list[Any]) -> list[Any]:
    return [node for node in nodes if node.job.labels.get(BUILDER_LABEL) == "true"]


def vm_init_options_for_autoscaled_node(
    node: SandboxNode,
    role: str,
    args: argparse.Namespace,
    config: DeploymentConfig,
) -> VmInitOptions:
    del args
    labels = dict(node.job.labels)
    if role == "builder":
        labels.pop(NODE_LABEL, None)
        labels.setdefault(BUILDER_LABEL, "true")
        package_spec = str(config.builder_node_package_bundle())
    else:
        labels.setdefault(NODE_LABEL, "true")
        package_spec = str(config.sandbox_node_package_bundle())
    labels.setdefault(DEPLOYMENT_LABEL, config.deployment_id)
    return vm_init_options_for_job(
        config,
        node.job,
        role,
        package_spec=package_spec,
        node_id=(
            node.job.hostname
            or (node.heartbeat.node_id if node.heartbeat is not None else "")
        ),
        labels=labels,
    )


def apply_bootstrap_requirements(intent: VmBootstrapIntent) -> VmBootstrapIntent:
    if not intent.runnable:
        return intent
    if not intent.options.heartbeat_url:
        return replace(
            intent,
            runnable=False,
            reason="deployment gateway_private_host is required",
        )
    if (
        intent.options.heartbeat_bearer_token_file
        and not intent.options.heartbeat_bearer_token
    ):
        return replace(
            intent,
            runnable=False,
            reason="deployment heartbeat token is required",
        )
    if (
        intent.options.node_control_bearer_token_file
        and not intent.options.node_control_bearer_token
    ):
        return replace(
            intent,
            runnable=False,
            reason="deployment node-control token is required",
        )
    return intent


def vm_bootstrap_intent_to_dict(intent: VmBootstrapIntent) -> dict[str, Any]:
    return {
        "jobId": intent.job_id,
        "nodeId": intent.node_id,
        "role": intent.role,
        "runnable": intent.runnable,
        "reason": intent.reason,
        "sshCommand": intent.access.command,
        "previousAttempts": intent.previous_attempts,
        "options": vm_init_options_to_dict(intent.options),
    }


def resources_from_vm_job(
    job: ProviderInstance, default: ResourceQuantity
) -> ResourceQuantity:
    return ResourceQuantity(
        vcpu=float(job.cpu) if job.cpu is not None else default.vcpu,
        memory_mb=(job.memory_gb * 1024)
        if job.memory_gb is not None
        else default.memory_mb,
        disk_mb=(job.disk_gb * 1024) if job.disk_gb is not None else default.disk_mb,
    )


def print_vm_job(job: ProviderInstance) -> None:
    print(f"Job: {job.id}")
    print(f"State: {job.state}")
    print(f"Application: {job.application_name}:{job.application_version}")
    print(f"Product: {job.product_id} ({job.product_category})")
    print(f"Machine: {job.cpu or '?'} vCPU, {job.memory_gb or '?'} GB RAM")
    print(f"Disk: {job.disk_gb or '?'} GB")
    print(f"Hostname: {job.hostname or ''}")
    print(f"SSH enabled: {job.ssh_enabled}")
    print(f"Private networks: {', '.join(job.private_network_ids)}")
    print(f"Queue status: {job.queue_status or ''}")
    if job.latest_note:
        print(f"Latest note: {job.latest_note}")


def print_plan(
    nodes: list[Any],
    decision: Any,
    control_state_file: Path,
    *,
    provider_kind: str,
    provider_scope_id: str,
    footer: str | None = "Dry-run only. Mutation commands are not implemented yet.",
) -> None:
    unreachable = getattr(decision, "unreachable_nodes", 0)
    unreachable_suffix = f", {unreachable} unreachable" if unreachable else ""
    print(f"Provider: {provider_kind} ({provider_scope_id})")
    print(f"Control state: {control_state_file}")
    print(
        "Nodes: "
        f"{decision.ready_nodes} ready, "
        f"{decision.provisioning_nodes} provisioning"
        f"{unreachable_suffix}, "
        f"{decision.total_nodes} total"
    )
    print(
        "Resources: "
        f"pending={resource_summary(decision.pending_resources.to_dict())}, "
        f"prepared={resource_summary(decision.prepared_resources.to_dict())}, "
        f"desired={resource_summary(decision.desired_resources.to_dict())}, "
        f"projected_free={resource_summary(decision.projected_free_resources.to_dict())}, "
        f"deficit={resource_summary(decision.resource_deficit.to_dict())}"
    )
    visible_nodes = [node for node in nodes if not node.job.is_final]
    if not visible_nodes:
        print("No pool nodes matched the configured selection.")
    for node in visible_nodes:
        heartbeat = "fresh" if node.heartbeat_fresh else "missing/stale"
        resource_suffix = ""
        if node.heartbeat is not None:
            resource_suffix = (
                f" used={resource_summary(node.heartbeat.used_resources.to_dict())}"
                f" free={resource_summary(node.heartbeat.free_resources.to_dict())}"
                f" total={resource_summary(node.heartbeat.total_resources.to_dict())}"
            )
        print(
            f"- job={node.job_id} state={node.state} "
            f"active_sandboxes={node.active_sandboxes} "
            f"heartbeat={heartbeat}{resource_suffix}"
        )
    print("Decision:")
    for action in decision.actions:
        if action.kind == "create":
            print(f"- create {action.count}: {action.reason}")
        elif action.kind == "stop":
            print(f"- stop {', '.join(action.job_ids)}: {action.reason}")
        else:
            print(f"- {action.kind}: {action.reason}")
    for reason in decision.reasons:
        print(f"Reason: {reason}")
    if footer:
        print(footer)


def print_reconcile(
    nodes: list[Any],
    decision: Any,
    control_state_file: Path,
    create_intents: list[InstanceCreateIntent],
    stop_job_ids: tuple[str, ...],
    result: dict[str, Any],
) -> None:
    provider = result["provider"]
    print_plan(
        nodes,
        decision,
        control_state_file,
        provider_kind=str(provider["kind"]),
        provider_scope_id=str(provider["scopeId"]),
        footer=None,
    )
    builder_decision = result.get("rawBuilderDecision")
    if builder_decision is not None:
        print("Builder decision:")
        for action in builder_decision.actions:
            if action.kind == "create":
                print(f"- create {action.count}: {action.reason}")
            elif action.kind == "stop":
                print(f"- stop {', '.join(action.job_ids)}: {action.reason}")
            else:
                print(f"- {action.kind}: {action.reason}")
        for reason in builder_decision.reasons:
            print(f"Builder reason: {reason}")
    print("Create intents:")
    if not create_intents:
        print("- none")
    for intent in create_intents:
        print(
            f"- {intent.name} ({intent.role}): host={intent.node_id} "
            f"url={intent.node_url}"
        )
    print("Stop intents:")
    requested_stop_job_ids = tuple(result.get("requestedStopJobIds", []))
    blocked_stop_job_ids = tuple(result.get("blockedStopJobIds", []))
    blocked_detach_job_ids = set(
        result.get("blocked_storage_native_detach_stop_job_ids", [])
    )
    if not requested_stop_job_ids:
        print("- none")
    for job_id in stop_job_ids:
        print(f"- {job_id}")
    for job_id in blocked_stop_job_ids:
        if job_id in blocked_detach_job_ids:
            print(f"- {job_id} (blocked: worker storage cannot detach safely)")
        else:
            print(f"- {job_id} (blocked: missing matching deployment label)")
    lost_job_ids = tuple(result.get("destructive_power_cycle_job_ids", []))
    print("Destructive node-loss intents:")
    if not lost_job_ids:
        print("- none")
    for job_id in lost_job_ids:
        print(f"- {job_id}")
    print("Bootstrap intents:")
    bootstrap_intents = result.get("rawBootstrapIntents", [])
    if not bootstrap_intents:
        print("- none")
    for intent in bootstrap_intents:
        status = "runnable" if intent.runnable else "blocked"
        print(
            f"- {intent.job_id} ({intent.role}, {status}): "
            f"node={intent.node_id} reason={intent.reason}"
        )
    bootstrap_results = result.get("bootstrapResults", [])
    for item in bootstrap_results:
        if item.get("skipped"):
            print(f"Skipped init for {item.get('jobId')}: {item.get('reason')}")
        else:
            print(
                f"Init {item.get('status')} for {item.get('jobId')}: "
                f"returncode={item.get('returncode')}"
            )
    submitted_operations = [
        item
        for item in result.get("providerOperationResults", [])
        if item.get("source") == "planned"
    ]
    if any(item.get("kind") == "create" for item in submitted_operations):
        created = result.get("createdJobIds", [])
        created_label = ", ".join(created) if created else "(none returned)"
        print(f"Submitted create jobs: {created_label}")
    elif create_intents:
        print("Create dry-run only. Re-run with --execute to submit planned VMs.")
    if any(item.get("kind") == "stop" for item in submitted_operations):
        print(f"Executed stop requests: {', '.join(stop_job_ids)}")
        if blocked_stop_job_ids:
            print(f"Skipped blocked stop requests: {', '.join(blocked_stop_job_ids)}")
    elif requested_stop_job_ids:
        if blocked_stop_job_ids:
            if result.get("execute"):
                if set(blocked_stop_job_ids) == blocked_detach_job_ids:
                    print(
                        "No stop requests executed. Parked storage-native state "
                        "cannot detach from its worker safely."
                    )
                else:
                    print(
                        "No stop requests executed. Some jobs lack a matching "
                        "deployment label or detachable published storage."
                    )
            else:
                print(
                    "Stop blocked: jobs require matching deployment and ownership "
                    "labels."
                )
        else:
            if result.get("execute"):
                print(
                    "Stop request is waiting for drain proof; no provider stop "
                    "was submitted this cycle."
                )
            else:
                print(
                    "Stop dry-run only. Re-run with --execute to terminate "
                    "planned jobs."
                )


def vm_job_to_dict(job: ProviderInstance) -> dict[str, Any]:
    raw = asdict(job)
    raw.pop("raw", None)
    for key in ("created_at", "started_at", "expires_at"):
        if raw[key] is not None:
            raw[key] = raw[key].isoformat()
    return raw


def node_to_dict(node: Any) -> dict[str, Any]:
    raw = {
        "job": vm_job_to_dict(node.job),
        "activeSandboxes": node.active_sandboxes,
        "heartbeatFresh": node.heartbeat_fresh,
        "ready": node.is_ready,
        "provisioning": node.is_provisioning,
        "unreachable": node.is_unreachable,
        "permanentlyLost": node.permanently_lost,
    }
    if node.heartbeat is not None:
        raw["heartbeat"] = heartbeat_to_dict(node.heartbeat)
    return raw


def scale_decision_to_dict(decision: Any) -> dict[str, Any]:
    return {
        "actions": [asdict(action) for action in decision.actions],
        "readyNodes": decision.ready_nodes,
        "provisioningNodes": decision.provisioning_nodes,
        "unreachableNodes": decision.unreachable_nodes,
        "totalNodes": decision.total_nodes,
        "pendingResources": decision.pending_resources.to_dict(),
        "suppressedPendingResources": (decision.suppressed_pending_resources.to_dict()),
        "pendingCount": decision.pending_count,
        "suppressedPendingCount": decision.suppressed_pending_count,
        "preparedResources": decision.prepared_resources.to_dict(),
        "desiredResources": decision.desired_resources.to_dict(),
        "projectedFreeResources": decision.projected_free_resources.to_dict(),
        "resourceDeficit": decision.resource_deficit.to_dict(),
        "liveSignals": (
            decision.live_signals.to_dict()
            if decision.live_signals is not None
            else None
        ),
        "programSignals": (
            decision.program_signals.to_dict()
            if decision.program_signals is not None
            else None
        ),
        "pressureScaleUp": decision.pressure_scale_up,
        "createPressureScaleUp": decision.create_pressure_scale_up,
        "effectiveScaleDownIdleSeconds": (decision.effective_scale_down_idle_seconds),
        "reasons": list(decision.reasons),
    }


def dashboard_scale_policy_to_dict(policy: ScalePolicy) -> dict[str, Any]:
    """Expose non-secret effective knobs needed to explain scale decisions."""

    return {
        "min_nodes": policy.min_nodes,
        "max_nodes": policy.max_nodes,
        "warm_resources": policy.warm_resources.to_dict(),
        "max_create_per_cycle": policy.max_create_per_cycle,
        "max_stop_per_cycle": policy.max_stop_per_cycle,
        "max_provisioning_nodes": policy.max_provisioning_nodes,
        "provisioning_capacity_weight": policy.provisioning_capacity_weight,
        "stale_provisioning_after_seconds": (policy.stale_provisioning_after_seconds),
        "stale_provisioning_capacity_weight": (
            policy.stale_provisioning_capacity_weight
        ),
        "unreachable_stop_after_seconds": policy.unreachable_stop_after_seconds,
        "scale_down_idle_seconds": policy.scale_down_idle_seconds,
        "builder_scale_down_idle_seconds": policy.builder_scale_down_idle_seconds,
        "heartbeat_ttl_seconds": policy.heartbeat_ttl_seconds,
        "live_pressure_enabled": policy.live_pressure_enabled,
        "live_pressure_window_seconds": policy.live_pressure_window_seconds,
        "live_pressure_min_samples": policy.live_pressure_min_samples,
        "live_pressure_fresh_seconds": policy.live_pressure_fresh_seconds,
        "target_cpu_utilization": policy.target_cpu_utilization,
        "target_memory_utilization": policy.target_memory_utilization,
        "max_memory_psi_full_avg10": policy.max_memory_psi_full_avg10,
        "target_storage_queue_utilization": (policy.target_storage_queue_utilization),
        "create_pressure_enabled": policy.create_pressure_enabled,
        "create_pressure_window_seconds": policy.create_pressure_window_seconds,
        "create_pressure_min_samples": policy.create_pressure_min_samples,
        "create_pressure_fresh_seconds": policy.create_pressure_fresh_seconds,
        "create_target_concurrency_per_node": (
            policy.create_target_concurrency_per_node
        ),
        "create_pressure_max_headroom_nodes": (
            policy.create_pressure_max_headroom_nodes
        ),
        "pressure_scale_down_cooldown_seconds": (
            policy.pressure_scale_down_cooldown_seconds
        ),
        "provisioning_latency_lookback_seconds": (
            policy.provisioning_latency_lookback_seconds
        ),
        "provisioning_scale_down_multiplier": (
            policy.provisioning_scale_down_multiplier
        ),
        "program_aware_autoscaling_enabled": (policy.program_aware_autoscaling_enabled),
        "model_wait_capacity_weight": policy.model_wait_capacity_weight,
        "model_wait_max_headroom_nodes": policy.model_wait_max_headroom_nodes,
        "default_node_resources": policy.default_node_resources.to_dict(),
    }


def print_json(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def parse_labels(raw_labels: list[str]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for raw in raw_labels:
        if "=" not in raw:
            raise ValueError(f"Invalid label {raw!r}. Use key=value.")
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid label {raw!r}. Label key cannot be empty.")
        labels[key] = value.strip()
    return labels


def compute_provider_from_args(
    args: argparse.Namespace,
    config: DeploymentConfig,
) -> ComputeProvider:
    """Compose the selected adapter at the CLI boundary.

    Built-in adapters are composed directly. Other providers are discovered by
    their tagged configuration kind through an entry point.
    """

    if config.provider.kind == "hetzner":
        from .providers.hetzner.composition import (
            provider_from_configuration as hetzner_provider_from_configuration,
        )

        return hetzner_provider_from_configuration(config.provider)
    if config.provider.kind != "ucloud":
        return load_external_provider(config.provider, args)
    return ucloud_provider_from_configuration(
        config.provider,
        session_file=ucloud_settings(config).session_file,
        sandbox_product_id=config.sandbox.product_id,
        sandbox_disk_gb=config.sandbox.disk_gb,
        builder_product_id=config.builder.product_id,
        builder_disk_gb=config.builder.disk_gb,
        client_factory=UCloudClient,
    )


def vm_submission_options_from_args(
    args: argparse.Namespace,
    config: DeploymentConfig,
) -> tuple[VmSubmissionOptions, str]:
    role = getattr(args, "role", "node")
    private_network_id: str | None
    if args.no_private_network:
        private_network_id = None
    else:
        private_network_id = (
            args.private_network_id or ucloud_settings(config).private_network_id
        )
        if not private_network_id:
            raise ValueError(
                "private network id is required via --private-network-id or config; "
                "use --no-private-network to submit without one."
            )

    if getattr(args, "no_public_link", False):
        public_link_id = None
    else:
        explicit_public_link_id = getattr(args, "public_link_id", None)
        public_link_id = explicit_public_link_id or (
            ucloud_settings(config).gateway_public_link_id
            if role == "gateway"
            else None
        )
    public_link_port = (
        getattr(args, "public_link_port", None)
        if getattr(args, "public_link_port", None) is not None
        else ucloud_settings(config).gateway_public_link_port
        or DEFAULT_PUBLIC_LINK_PORT
    )

    seed = args.hostname_seed or uuid4().hex[:8]
    hostname_prefix = args.hostname_prefix or (
        "sandbox-gateway"
        if role == "gateway"
        else "sandbox-builder"
        if role == "builder"
        else "sandbox-node"
    )
    hostname = args.hostname or stable_hostname(seed, prefix=hostname_prefix)
    if role == "gateway":
        default_name_prefix = "ucloud-sandbox-gateway"
    elif role == "builder":
        default_name_prefix = "ucloud-sandbox-builder"
    else:
        default_name_prefix = "ucloud-sandbox-node"
    name = args.name or stable_hostname(seed, prefix=default_name_prefix)
    labels = parse_labels(args.label)
    if role == "gateway":
        labels.setdefault(GATEWAY_LABEL, "true")
    elif role == "builder":
        labels.setdefault(BUILDER_LABEL, "true")
    else:
        labels.setdefault(NODE_LABEL, "true")
    if config.deployment_id:
        labels.setdefault(DEPLOYMENT_LABEL, config.deployment_id)
    labels.setdefault(AGENT_VERSION_LABEL, package_version())
    labels.setdefault(INIT_VERSION_LABEL, DEFAULT_INIT_VERSION)
    ssh_requested = bool(getattr(args, "ssh", False))
    ssh_disabled = bool(getattr(args, "no_ssh", False))
    if ssh_requested and ssh_disabled:
        raise ValueError("--ssh and --no-ssh cannot be used together.")
    file_mounts = tuple(file_mounts_from_args(args))
    product_id = args.product_id or (
        DEFAULT_GATEWAY_VM_PRODUCT_ID if role == "gateway" else DEFAULT_VM_PRODUCT_ID
    )

    return (
        VmSubmissionOptions(
            name=name,
            hostname=hostname,
            private_network_id=private_network_id,
            public_link_id=public_link_id,
            public_link_port=public_link_port,
            product=VmProductRef(
                id=product_id,
                category=args.product_category,
                provider=args.product_provider,
            ),
            application=VmApplicationRef(
                name=args.app_name,
                version=args.app_version,
            ),
            disk_gb=args.disk_gb,
            time_allocation=VmTimeAllocation(
                hours=args.time_hours,
                minutes=args.time_minutes,
                seconds=args.time_seconds,
            ),
            ssh_enabled=ssh_requested,
            allow_duplicate_job=args.allow_duplicate_job,
            labels=labels,
            file_mounts=file_mounts,
        ),
        seed,
    )


def file_mounts_from_args(args: argparse.Namespace) -> list[VmFileMount]:
    mounts = [
        VmFileMount(path=str(path), read_only=False)
        for path in getattr(args, "mount", []) or []
    ]
    mounts.extend(
        VmFileMount(path=str(path), read_only=True)
        for path in getattr(args, "mount_ro", []) or []
    )
    return mounts


def submitted_job_ids(response: dict[str, Any]) -> list[str]:
    responses = response.get("responses")
    if not isinstance(responses, list):
        return []
    ids: list[str] = []
    for item in responses:
        if not isinstance(item, dict):
            continue
        job_id = item.get("id")
        if isinstance(job_id, str) and job_id:
            ids.append(job_id)
    return ids


def read_bearer_token_source(
    *,
    token_file: str,
    source_file: Path | None,
) -> str:
    path = source_file
    if path is None and token_file:
        candidate = Path(token_file).expanduser()
        if candidate.is_file():
            path = candidate
    if path is None:
        return ""
    return path.read_text(encoding="utf-8").strip()


def read_required_token_file(path: Path | None, label: str) -> str | None:
    if path is None:
        return None
    token = path.read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError(f"{label} file is empty: {path}")
    return token


PUBLIC_SSH_KEY_PREFIXES = (
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
    "sk-ecdsa-sha2-nistp256@openssh.com",
    "sk-ssh-ed25519@openssh.com",
    "ssh-ed25519",
    "ssh-rsa",
)


def read_public_ssh_key_file(path: Path) -> str:
    public_key = path.read_text(encoding="utf-8").strip()
    if not public_key:
        raise ValueError("public key file is empty.")
    if "\n" in public_key or "\r" in public_key:
        raise ValueError("public key file must contain exactly one public key.")
    if not any(
        public_key.startswith(prefix + " ") for prefix in PUBLIC_SSH_KEY_PREFIXES
    ):
        raise ValueError("public key file does not look like an OpenSSH public key.")
    return public_key


def find_ucloud_ssh_key(
    items: list[dict[str, Any]], public_key: str
) -> dict[str, Any] | None:
    for item in items:
        specification = item.get("specification")
        if isinstance(specification, dict) and specification.get("key") == public_key:
            return item
    return None


def vm_init_options_for_job(
    config: DeploymentConfig,
    job: ProviderInstance,
    role: str,
    *,
    package_spec: str,
    package_sha256: str = "",
    node_id: str = "",
    labels: dict[str, str] | None = None,
) -> VmInitOptions:
    if role not in {"sandbox", "builder"}:
        raise ValueError("VM init role must be sandbox or builder")
    heartbeat_token_file = config.heartbeat_token_file()
    node_control_token_file = config.node_control_token_file()
    resolved_node_id = node_id or job.hostname or f"ucloud-vm-{job.id}"
    resources = resources_from_vm_job(job, config.sandbox.resources)
    if role == "builder" and resources.disk_mb > 0:
        resources = replace(
            resources,
            disk_mb=min(
                resources.disk_mb,
                config.builder.docker_quota_image_gb * 1024,
            ),
        )
    authorized_key_file = config.init_authorized_key_file()
    authorized_keys = (
        (read_public_ssh_key_file(authorized_key_file),)
        if authorized_key_file.is_file()
        else ()
    )
    host_alias = config.registry_host_alias
    snapshot_store = config.snapshot_store
    s3_access_key_id = ""
    s3_secret_access_key = ""
    s3_security_token = ""
    if role == "sandbox" and snapshot_store.kind == "s3":
        s3_access_key_id = os.environ.get(snapshot_store.access_key_id_env, "").strip()
        s3_secret_access_key = os.environ.get(
            snapshot_store.secret_access_key_env, ""
        ).strip()
        s3_security_token = os.environ.get(
            snapshot_store.security_token_env, ""
        ).strip()
        if not s3_access_key_id or not s3_secret_access_key:
            raise ValueError(
                "S3 snapshot credentials are missing from "
                f"{snapshot_store.access_key_id_env} and "
                f"{snapshot_store.secret_access_key_env}"
            )
    return VmInitOptions(
        job_id=job.id,
        heartbeat_url=config.heartbeat_url,
        role=role,
        heartbeat_bearer_token_file=str(heartbeat_token_file),
        heartbeat_bearer_token=read_bearer_token_source(
            token_file=str(heartbeat_token_file),
            source_file=heartbeat_token_file,
        ),
        node_control_bearer_token_file=str(node_control_token_file),
        node_control_bearer_token=read_bearer_token_source(
            token_file=str(node_control_token_file),
            source_file=node_control_token_file,
        ),
        service_user="ucloud",
        init_authorized_keys=authorized_keys,
        node_id=resolved_node_id,
        work_dir=DEFAULT_INSTALL_ROOT,
        state_dir=(
            DEFAULT_UCLOUD_NODE_STATE_DIR if config.provider.kind == "ucloud" else ""
        ),
        package_spec=package_spec,
        package_sha256=package_sha256,
        node_agent_host="0.0.0.0",
        node_agent_port=8090,
        deployment_id=config.deployment_id,
        telemetry_otlp_endpoint=config.telemetry.endpoint,
        telemetry_cloud_provider=config.provider.kind,
        telemetry_cloud_machine_type=job.product_id,
        telemetry_trace_sample_ratio=config.telemetry.trace_sample_ratio,
        telemetry_export_interval_ms=config.telemetry.export_interval_ms,
        telemetry_export_timeout_ms=config.telemetry.export_timeout_ms,
        telemetry_max_queue_size=config.telemetry.max_queue_size,
        telemetry_max_export_batch_size=config.telemetry.max_export_batch_size,
        ssh_port_start=22000,
        ssh_port_end=22999,
        total_resources=resources,
        docker_quota_image_gb=(
            config.builder.docker_quota_image_gb
            if role == "builder"
            else config.sandbox.docker_quota_image_gb
        ),
        swap_gb=0 if role == "builder" else config.sandbox.swap_gb,
        docker_insecure_registries=(
            f"{config.registry_endpoint_host}:{config.registry_port}",
        ),
        host_aliases=(host_alias,) if host_alias else (),
        buildx_cache_ref=(config.builder.buildx_cache_ref if role == "builder" else ""),
        direct_runsc_commit=(
            config.sandbox.direct_runsc_commit if role == "sandbox" else ""
        ),
        direct_network="sandbox" if role == "sandbox" else "none",
        direct_network_allow_tcp=(
            config.sandbox.direct_network_allow_tcp if role == "sandbox" else ()
        ),
        storage_native_registry_url=(
            config.registry_worker_url if role == "sandbox" else ""
        ),
        storage_native_repository=config.sandbox.storage_native_repository,
        storage_native_snapshot_backend=(
            snapshot_store.kind if role == "sandbox" else "registry"
        ),
        storage_native_s3_endpoint=(
            snapshot_store.endpoint if role == "sandbox" else ""
        ),
        storage_native_s3_bucket=(snapshot_store.bucket if role == "sandbox" else ""),
        storage_native_s3_region=(snapshot_store.region if role == "sandbox" else ""),
        storage_native_s3_prefix=(snapshot_store.prefix if role == "sandbox" else ""),
        storage_native_s3_access_key_id=s3_access_key_id,
        storage_native_s3_secret_access_key=s3_secret_access_key,
        storage_native_s3_security_token=s3_security_token,
        storage_native_cache_gb=config.sandbox.storage_native_cache_gb,
        storage_native_pool_low_watermark=(
            config.sandbox.storage_native_pool_low_watermark
        ),
        storage_native_pool_high_watermark=(
            config.sandbox.storage_native_pool_high_watermark
        ),
        storage_native_max_ublk_devices=(
            DEFAULT_UCLOUD_STORAGE_NATIVE_MAX_UBLK_DEVICES
            if config.provider.kind == "ucloud" and role == "sandbox"
            else DEFAULT_STORAGE_NATIVE_MAX_UBLK_DEVICES
        ),
        direct_disk_headroom_mb=config.sandbox.direct_disk_headroom_mb,
        direct_max_concurrent_restores=(config.sandbox.direct_max_concurrent_restores),
        direct_idle_park_seconds=config.sandbox.direct_idle_park_seconds,
        max_concurrent_image_pulls=(
            config.builder.max_concurrent_image_pulls
            if role == "builder"
            else config.sandbox.max_concurrent_image_pulls
        ),
        heartbeat_interval_seconds=config.heartbeat_interval_seconds,
        labels=dict(labels if labels is not None else job.labels),
    )


def vm_init_options_to_dict(options: VmInitOptions) -> dict[str, Any]:
    return {
        "jobId": options.job_id,
        "nodeId": options.normalized_node_id(),
        "heartbeatUrl": options.heartbeat_url,
        "heartbeatBearerTokenFile": options.heartbeat_bearer_token_file,
        "nodeControlBearerTokenFile": options.node_control_bearer_token_file,
        "serviceUser": options.service_user,
        "initAuthorizedKeys": list(options.init_authorized_keys),
        "workDir": options.work_dir,
        "stateDir": options.state_dir,
        "packageSpec": options.package_spec,
        "packageSha256": options.package_sha256,
        "nodeAgentHost": options.node_agent_host,
        "nodeAgentPort": options.node_agent_port,
        "nodeUrl": options.advertised_node_url(),
        "deploymentId": options.deployment_id,
        "telemetryOtlpEndpoint": options.telemetry_otlp_endpoint,
        "telemetryCloudProvider": options.telemetry_cloud_provider,
        "telemetryCloudMachineType": options.telemetry_cloud_machine_type,
        "telemetryTraceSampleRatio": options.telemetry_trace_sample_ratio,
        "telemetryExportIntervalMs": options.telemetry_export_interval_ms,
        "telemetryExportTimeoutMs": options.telemetry_export_timeout_ms,
        "telemetryMaxQueueSize": options.telemetry_max_queue_size,
        "telemetryMaxExportBatchSize": options.telemetry_max_export_batch_size,
        "sshPortStart": options.ssh_port_start,
        "sshPortEnd": options.ssh_port_end,
        "totalResources": options.total_resources.to_dict(),
        "dockerQuotaImageGb": options.docker_quota_image_gb,
        "swapGb": options.swap_gb,
        "dockerInsecureRegistries": list(options.docker_insecure_registries),
        "hostAliases": list(options.host_aliases),
        "role": options.role,
        "directRunscCommit": options.direct_runsc_commit,
        "directNetwork": options.direct_network,
        "directNetworkAllowTcp": list(options.direct_network_allow_tcp),
        "storageNativeRegistryUrl": options.storage_native_registry_url,
        "storageNativeRepository": options.storage_native_repository,
        "storageNativeSnapshotBackend": options.storage_native_snapshot_backend,
        "storageNativeS3Endpoint": options.storage_native_s3_endpoint,
        "storageNativeS3Bucket": options.storage_native_s3_bucket,
        "storageNativeS3Region": options.storage_native_s3_region,
        "storageNativeS3Prefix": options.storage_native_s3_prefix,
        "storageNativeCacheGb": options.storage_native_cache_gb,
        "storageNativePoolLowWatermark": (options.storage_native_pool_low_watermark),
        "storageNativePoolHighWatermark": (options.storage_native_pool_high_watermark),
        "storageNativeMaxUblkDevices": options.storage_native_max_ublk_devices,
        "directDiskHeadroomMb": options.direct_disk_headroom_mb,
        "directMaxConcurrentRestores": options.direct_max_concurrent_restores,
        "directIdleParkSeconds": options.direct_idle_park_seconds,
        "maxConcurrentImagePulls": options.max_concurrent_image_pulls,
        "heartbeatIntervalSeconds": options.heartbeat_interval_seconds,
        "capabilities": list(options.capabilities()),
        "labels": dict(options.labels or {}),
    }


def resource_quantity_from_args(args: argparse.Namespace) -> ResourceQuantity:
    return ResourceQuantity(
        vcpu=max(0.0, float(getattr(args, "total_vcpu", 0.0))),
        memory_mb=max(0, int(getattr(args, "total_memory_mb", 0))),
        disk_mb=max(0, int(getattr(args, "total_disk_mb", 0))),
    )


def resource_summary(raw: dict[str, Any]) -> str:
    return (
        f"{raw.get('vcpu', 0)}vcpu/"
        f"{raw.get('memory_mb', 0)}MB/"
        f"{raw.get('disk_mb', 0)}MB"
    )


if __name__ == "__main__":
    raise SystemExit(main())
