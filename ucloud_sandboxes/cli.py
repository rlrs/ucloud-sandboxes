from __future__ import annotations

import argparse
from dataclasses import asdict
from dataclasses import replace
from datetime import timedelta
import json
import os
from pathlib import Path
import sys
import time
from typing import Any
from uuid import uuid4

from .agent import (
    build_heartbeat,
    default_node_id,
    detect_job_id,
    fetch_node_agent_heartbeat,
    post_heartbeat,
    post_heartbeat_with_headers,
)
from .capabilities import (
    DISK_QUOTA_CAPABILITY,
    TMPFS_QUOTA_PROBE,
    conformance_capabilities_from_file,
    conformance_results_from_file,
    has_capability,
    merge_capabilities,
)
from .bootstrap import (
    VmBootstrapIntent,
    VmBootstrapStore,
    build_vm_bootstrap_intents,
    mark_bootstrap_attempt,
    mark_bootstrap_failure,
    mark_bootstrap_success,
    prune_bootstrap_records,
)
from .async_node_agent import create_async_node_agent_app
from .config import AutoscalerConfig
from .control_plane import DEFAULT_MAX_CONCURRENT_SANDBOX_CREATES, build_server
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
    DEFAULT_PROJECT_MOUNT_DIR,
    DEFAULT_REGISTRY_ALIAS,
    read_remote_text_over_ssh,
    render_remote_deploy_script,
    run_remote_script_over_ssh,
    stage_file_over_ssh,
)
from .images import DockerImageRuntime
from .managed_registry import (
    RegistryClient,
    execute_registry_prune,
    list_registry_tags,
    registry_prune_plan,
    select_prune_candidates,
)
from .metrics import (
    MetricsStore,
    record_autoscaler_cycle,
    record_vm_init_attempt,
    record_vm_observed,
    record_vm_submitted,
)
from .model_relay import create_model_relay_app
from .models import (
    ResourceQuantity,
    SandboxDemand,
    SandboxNode,
    ScalePolicy,
    VmJob,
    utc_now,
    vm_job_from_payload,
)
from .networking import (
    DEFAULT_PUBLIC_LINK_PORT,
    PrivateNetworkAttachment,
    PublicLinkAttachment,
    apply_private_network_attachment,
    apply_public_link_attachment,
    stable_hostname,
)
from .node_agent import build_node_agent_server
from .policy import evaluate_scale
from .reconcile import (
    VmCreateIntent,
    VmNodeSubmissionDefaults,
    build_builder_vm_create_intents,
    build_vm_create_intents,
    bulk_payload_from_create_intents,
    evaluate_builder_scale,
    partition_safe_stop_job_ids,
    stop_job_ids_from_decision,
)
from .registry import (
    HeartbeatStore,
    heartbeat_to_dict,
    load_heartbeats,
    merge_jobs_and_heartbeats,
)
from .routing import RoutingStore
from .runtime_probe import DockerRuntimeProbe
from .sandbox import DockerGvisorRuntime
from .ucloud import SessionStore, UCloudClient, UCloudError
from .vm_init import (
    DEFAULT_DOCKER_QUOTA_IMAGE_GB,
    VmInitOptions,
    plan_vm_init,
    render_vm_init_script,
    run_init_over_ssh,
    stage_vm_init_package_over_ssh,
)
from .vm_submit import (
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
)


DEFAULT_BUILDER_PRODUCT_ID = "cpu-amd-zen5-16-vcpu"
DEFAULT_BUILDER_DISK_GB = 250


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (OSError, ValueError, UCloudError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ucloud-sandboxes",
        description="Autoscale gVisor sandbox nodes backed by UCloud VM jobs.",
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
    inspect_job.add_argument("job_id")
    inspect_job.add_argument("--project", help="UCloud project id.")
    inspect_job.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    inspect_job.set_defaults(func=cmd_inspect_job)

    agent_heartbeat = subparsers.add_parser(
        "agent-heartbeat",
        help="Emit or submit one VM node heartbeat.",
    )
    add_config_args(agent_heartbeat)
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
    agent_heartbeat.add_argument(
        "--runtime-conformance-file",
        type=Path,
        help="Runtime conformance JSON used to derive security capabilities.",
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
        "--heartbeat-file",
        type=Path,
        help="Local heartbeat file to upsert into. Defaults to config state only when supplied.",
    )
    agent_heartbeat.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    agent_heartbeat.set_defaults(func=cmd_agent_heartbeat)

    serve = subparsers.add_parser(
        "serve-control-plane",
        help="Run the local heartbeat receiver for VM node agents.",
    )
    add_config_args(serve)
    serve.add_argument("--host", default="127.0.0.1", help="Bind host.")
    serve.add_argument("--port", type=int, default=8080, help="Bind port.")
    serve.add_argument(
        "--heartbeat-file",
        type=Path,
        help="Heartbeat state file. Defaults to <state_dir>/heartbeats.json.",
    )
    serve.add_argument(
        "--route-file",
        type=Path,
        help=(
            "Gateway route recovery and pending-demand database. "
            "Defaults to <state_dir>/routes.sqlite."
        ),
    )
    serve.add_argument(
        "--heartbeat-ttl-seconds",
        type=int,
        help="Freshness window for schedulable node heartbeats.",
    )
    serve.add_argument(
        "--max-concurrent-sandbox-creates",
        type=int,
        default=DEFAULT_MAX_CONCURRENT_SANDBOX_CREATES,
        help=(
            "Maximum concurrent sandbox create requests handled by the gateway. "
            "Set 0 to disable gateway create backpressure."
        ),
    )
    serve.add_argument(
        "--gateway-upstream-node-url",
        help=(
            "Proxy node-agent JSON API requests to this private-network node URL. "
            "If omitted, the gateway routes across nodes from heartbeat state."
        ),
    )
    serve.add_argument(
        "--gateway-bearer-token-file",
        type=Path,
        help=(
            "Require a gateway token for control-plane routes. The gateway accepts "
            "X-UCloud-Sandbox-Token, plus Authorization: Bearer for private callers."
        ),
    )
    serve.add_argument(
        "--image-file",
        type=Path,
        help="Control-plane image build state file. Defaults to <state_dir>/images.json.",
    )
    serve.add_argument(
        "--metrics-file",
        type=Path,
        help="JSONL metrics event file. Defaults to <state_dir>/metrics.jsonl.",
    )
    serve.add_argument(
        "--registry-url",
        help=(
            "Docker Distribution registry URL to include in gateway metrics. "
            "Defaults to UCLOUD_SANDBOX_REGISTRY_URL or UCLOUD_REGISTRY_URL."
        ),
    )
    serve.add_argument(
        "--docker-binary",
        default="docker",
        help="Docker-compatible CLI binary used for control-plane image builds.",
    )
    serve.add_argument(
        "--enable-image-builds",
        action="store_true",
        help="Allow this control-plane process to build images locally.",
    )
    serve.add_argument(
        "--execute-image-builds",
        action="store_true",
        help="Actually execute control-plane Docker image build/push commands.",
    )
    serve.set_defaults(func=cmd_serve_control_plane)

    node_agent = subparsers.add_parser(
        "serve-node-agent",
        help="Run the VM-side sandbox node agent API.",
    )
    add_config_args(node_agent)
    node_agent.add_argument("--host", default="127.0.0.1", help="Bind host.")
    node_agent.add_argument("--port", type=int, default=8090, help="Bind port.")
    node_agent.add_argument("--job-id", help="UCloud VM job id.")
    node_agent.add_argument("--node-id", help="Stable node id. Defaults to hostname.")
    node_agent.add_argument(
        "--node-url",
        help="URL advertised in heartbeats for control-plane/node-agent calls.",
    )
    add_node_version_args(node_agent)
    add_resource_args(node_agent)
    node_agent.add_argument(
        "--sandbox-file",
        type=Path,
        help="Sandbox state file. Defaults to <state_dir>/sandboxes.json.",
    )
    node_agent.add_argument(
        "--image-file",
        type=Path,
        help="Image cache state file. Defaults to <state_dir>/images.json.",
    )
    node_agent.add_argument(
        "--ssh-port-start",
        type=int,
        default=22000,
        help="First local host port available for per-sandbox SSH.",
    )
    node_agent.add_argument(
        "--ssh-port-end",
        type=int,
        default=22999,
        help="Last local host port available for per-sandbox SSH.",
    )
    node_agent.add_argument(
        "--docker-binary",
        default="docker",
        help="Docker-compatible CLI binary.",
    )
    node_agent.add_argument(
        "--runtime-name",
        default="runsc",
        help="Docker runtime name for gVisor/runsc.",
    )
    node_agent.add_argument(
        "--enable-image-builds",
        action="store_true",
        help=(
            "Manual/debug builder mode. The node advertises image-build but "
            "not sandbox capacity; production builds should run on the control plane."
        ),
    )
    node_agent.add_argument(
        "--runtime-conformance-file",
        type=Path,
        help="Runtime conformance JSON used to derive security capabilities.",
    )
    node_agent.add_argument(
        "--execute-runtime",
        action="store_true",
        help="Actually execute Docker commands. Default is dry-run.",
    )
    node_agent.set_defaults(func=cmd_serve_node_agent)

    async_node_agent = subparsers.add_parser(
        "serve-async-node-agent",
        help="Run the high-performance async VM-side exec/SSH node-agent API.",
    )
    add_config_args(async_node_agent)
    async_node_agent.add_argument("--host", default="127.0.0.1", help="Bind host.")
    async_node_agent.add_argument("--port", type=int, default=8091, help="Bind port.")
    async_node_agent.add_argument(
        "--sandbox-file",
        type=Path,
        help="Sandbox state file. Defaults to <state_dir>/sandboxes.json.",
    )
    async_node_agent.add_argument(
        "--image-file",
        type=Path,
        help="Image cache state file. Defaults to <state_dir>/images.json.",
    )
    async_node_agent.add_argument(
        "--ssh-port-start",
        type=int,
        default=22000,
        help="First local host port available for per-sandbox SSH.",
    )
    async_node_agent.add_argument(
        "--ssh-port-end",
        type=int,
        default=22999,
        help="Last local host port available for per-sandbox SSH.",
    )
    async_node_agent.add_argument(
        "--docker-binary",
        default="docker",
        help="Docker-compatible CLI binary.",
    )
    async_node_agent.add_argument(
        "--runtime-name",
        default="runsc",
        help="Docker runtime name for gVisor/runsc.",
    )
    async_node_agent.add_argument(
        "--execute-runtime",
        action="store_true",
        help="Actually execute Docker commands. Default is dry-run.",
    )
    async_node_agent.set_defaults(func=cmd_serve_async_node_agent)

    model_relay = subparsers.add_parser(
        "serve-model-relay",
        help="Run an outbound-only OpenAI-compatible model-call relay.",
    )
    model_relay.add_argument("--host", default="127.0.0.1", help="Bind host.")
    model_relay.add_argument("--port", type=int, default=8092, help="Bind port.")
    model_relay.add_argument(
        "--sandbox-bearer-token-file",
        type=Path,
        help=(
            "Require this bearer token for sandbox OpenAI-compatible requests. "
            "Use the token value as OPENAI_API_KEY."
        ),
    )
    model_relay.add_argument(
        "--worker-bearer-token-file",
        type=Path,
        help="Require this bearer token for worker register/poll/respond routes.",
    )
    model_relay.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=3600.0,
        help="Maximum time a sandbox model request waits for a worker response.",
    )
    model_relay.add_argument(
        "--worker-poll-timeout-seconds",
        type=float,
        default=30.0,
        help="Default long-poll timeout for /worker/poll.",
    )
    model_relay.add_argument(
        "--worker-lease-seconds",
        type=float,
        default=600.0,
        help="How long a polled request is reserved for one worker before retry.",
    )
    model_relay.add_argument(
        "--completed-request-retention-seconds",
        type=float,
        default=3600.0,
        help="How long completed request ids are retained for idempotent responses.",
    )
    model_relay.set_defaults(func=cmd_serve_model_relay)

    runtime_conformance = subparsers.add_parser(
        "runtime-conformance",
        help="Check local Docker/runsc sandbox runtime behavior on an initialized node.",
    )
    runtime_conformance.add_argument(
        "--docker-binary",
        default="docker",
        help="Docker-compatible CLI binary.",
    )
    runtime_conformance.add_argument(
        "--sudo",
        action="store_true",
        help="Prefix probe Docker commands with sudo.",
    )
    runtime_conformance.add_argument(
        "--runtime-name",
        default="runsc",
        help="Docker runtime name for gVisor/runsc.",
    )
    runtime_conformance.add_argument(
        "--image",
        default="busybox",
        help="Small image used for runtime probes.",
    )
    runtime_conformance.add_argument(
        "--execute",
        action="store_true",
        help="Execute probes. Default renders the probe commands without running them.",
    )
    runtime_conformance.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    runtime_conformance.set_defaults(func=cmd_runtime_conformance)

    render_init = subparsers.add_parser(
        "render-vm-init-script",
        help="Render the post-boot VM init script.",
    )
    add_config_args(render_init)
    add_vm_init_args(render_init, include_job_id=True)
    render_init.add_argument(
        "--output", choices=("script", "json"), default="script", help="Output format."
    )
    render_init.set_defaults(func=cmd_render_vm_init_script)

    init_vm = subparsers.add_parser(
        "init-vm",
        help="Plan or execute post-boot init for a running UCloud VM job.",
    )
    add_config_args(init_vm)
    init_vm.add_argument("job_id", help="UCloud VM job id.")
    init_vm.add_argument("--project", help="UCloud project id.")
    add_vm_init_args(init_vm, include_job_id=False)
    init_vm.add_argument(
        "--execute",
        action="store_true",
        help="Run the init script over the announced SSH command. Default is dry-run.",
    )
    init_vm.add_argument(
        "--timeout-seconds",
        type=int,
        default=1800,
        help="Timeout for remote init execution.",
    )
    init_vm.add_argument(
        "--ssh-private-key-file",
        help="Private key file passed to ssh when executing VM init.",
    )
    init_vm.add_argument(
        "--output",
        choices=("text", "json", "script"),
        default="text",
        help="Output format.",
    )
    init_vm.set_defaults(func=cmd_init_vm)

    ensure_ssh_key = subparsers.add_parser(
        "ensure-ucloud-ssh-key",
        help="Create a UCloud account SSH key if the public key is not already registered.",
    )
    add_config_args(ensure_ssh_key)
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
        help="Prefix used with --hostname-seed. Defaults to config.node_hostname_prefix.",
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
        help="UCloud public link resource id. Defaults to config.gateway_public_link_id.",
    )
    public_link_attachment.add_argument(
        "--port",
        type=int,
        help=(
            "VM-local port exposed through the public link. Defaults to "
            f"config.gateway_public_link_port or {DEFAULT_PUBLIC_LINK_PORT}."
        ),
    )
    public_link_attachment.add_argument(
        "--output", choices=("text", "json"), default="json", help="Output format."
    )
    public_link_attachment.set_defaults(func=cmd_vm_public_link_attachment)

    registry_prune = subparsers.add_parser(
        "registry-prune",
        help="Plan or delete old tags from a Docker registry.",
    )
    registry_prune.add_argument(
        "--registry-url",
        default="http://127.0.0.1:5000",
        help="Base URL for the registry API.",
    )
    registry_prune.add_argument(
        "--keep-per-repository",
        type=int,
        default=5,
        help="Number of newest tags to retain per repository.",
    )
    registry_prune.add_argument(
        "--repository-prefix",
        default="",
        help="Only consider repositories with this prefix.",
    )
    registry_prune.add_argument(
        "--execute",
        action="store_true",
        help="Delete selected manifests. Without this flag, only print the plan.",
    )
    registry_prune.set_defaults(func=cmd_registry_prune)

    submit_vm = subparsers.add_parser(
        "submit-vm",
        help="Render or submit one UCloud VM job, including gateway VMs.",
    )
    add_config_args(submit_vm)
    submit_vm.add_argument("--project", help="UCloud project id.")
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
        help="Hostname prefix. Defaults to config.node_hostname_prefix.",
    )
    submit_vm.add_argument(
        "--private-network-id",
        help="UCloud private network id. Defaults to config.private_network_id.",
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
            f"config.gateway_public_link_port or {DEFAULT_PUBLIC_LINK_PORT}."
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
        default=DEFAULT_VM_PRODUCT_ID,
        help="UCloud VM product id.",
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
    open_vm_web.add_argument("job_id", help="UCloud VM job id.")
    open_vm_web.add_argument("--project", help="UCloud project id.")
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
        help=(
            "Converge a running gateway VM into the all-in-one deployment: "
            "gateway, relay, registry, and autoscaler."
        ),
    )
    add_config_args(deploy_all)
    deploy_all.add_argument("job_id", help="Running UCloud gateway VM job id.")
    deploy_all.add_argument("--project", help="UCloud project id.")
    deploy_all.add_argument(
        "--wheel",
        required=True,
        type=Path,
        help="Built ucloud-sandboxes wheel to install on the gateway VM.",
    )
    deploy_all.add_argument(
        "--ssh-command",
        help=(
            "SSH command for the gateway VM. If omitted, the command is read from "
            "UCloud job updates."
        ),
    )
    deploy_all.add_argument(
        "--ssh-private-key-file",
        help="Private key file passed to ssh/scp operations.",
    )
    deploy_all.add_argument(
        "--private-network-id",
        help="Private network id used by autoscaled sandbox and builder nodes.",
    )
    deploy_all.add_argument(
        "--gateway-private-host",
        help="Private-network hostname used by autoscaled nodes to reach the gateway.",
    )
    deploy_all.add_argument(
        "--registry-private-ip",
        help=(
            "Optional private-network IP override for the all-in-one VM. If "
            "omitted, the remote deployment detects the VM's private IPv4 and "
            "uses it in node init as ucloud-sandbox-registry=<ip>."
        ),
    )
    deploy_all.add_argument(
        "--registry-alias",
        default=DEFAULT_REGISTRY_ALIAS,
        help="Stable hostname used in private registry tags.",
    )
    deploy_all.add_argument("--install-root", default=DEFAULT_INSTALL_ROOT)
    deploy_all.add_argument("--project-mount-dir", default=DEFAULT_PROJECT_MOUNT_DIR)
    deploy_all.add_argument("--service-user", default="ucloud")
    deploy_all.add_argument("--gateway-port", type=int, default=8090)
    deploy_all.add_argument("--relay-port", type=int, default=8092)
    deploy_all.add_argument("--registry-port", type=int, default=5000)
    deploy_all.add_argument("--sandbox-product-id", default="cpu-amd-zen5-16-vcpu")
    deploy_all.add_argument("--sandbox-disk-gb", type=int, default=250)
    deploy_all.add_argument("--sandbox-idle-seconds", type=int, default=600)
    deploy_all.add_argument("--builder-product-id", default=DEFAULT_BUILDER_PRODUCT_ID)
    deploy_all.add_argument(
        "--builder-disk-gb", type=int, default=DEFAULT_BUILDER_DISK_GB
    )
    deploy_all.add_argument("--builder-idle-seconds", type=int, default=900)
    deploy_all.add_argument("--max-builder-nodes", type=int, default=1)
    deploy_all.add_argument("--autoscaler-interval-seconds", type=float, default=5.0)
    deploy_all.add_argument("--cpu-overcommit", type=float, default=2.0)
    deploy_all.add_argument("--memory-overcommit", type=float, default=1.2)
    deploy_all.add_argument("--disk-overcommit", type=float, default=1.0)
    deploy_all.add_argument("--docker-quota-image-gb", type=int, default=200)
    deploy_all.add_argument(
        "--ssh-key-title",
        help=(
            "Title used when registering the generated gateway init public key "
            "with UCloud."
        ),
    )
    deploy_all.add_argument(
        "--no-copy-session",
        action="store_true",
        help="Do not copy the local UCloud session file to the gateway VM.",
    )
    deploy_all.add_argument(
        "--no-open-public-links",
        action="store_true",
        help="Skip UCloud VM web-session activation for gateway and relay ports.",
    )
    deploy_all.add_argument(
        "--timeout-seconds",
        type=int,
        default=1800,
        help="Timeout for each remote staging or install operation.",
    )
    deploy_all.add_argument(
        "--execute",
        action="store_true",
        help="Stage files and run the remote deployment. Default is dry-run.",
    )
    deploy_all.add_argument(
        "--output",
        choices=("text", "json", "script"),
        default="text",
        help="Output format. script prints the remote install script.",
    )
    deploy_all.set_defaults(func=cmd_deploy_all_in_one)

    heartbeats = subparsers.add_parser(
        "heartbeats",
        help="List stored node heartbeats.",
    )
    add_config_args(heartbeats)
    heartbeats.add_argument(
        "--heartbeat-file",
        type=Path,
        help="Heartbeat state file. Defaults to <state_dir>/heartbeats.json.",
    )
    heartbeats.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    heartbeats.set_defaults(func=cmd_heartbeats)

    plan = subparsers.add_parser(
        "plan", help="Plan one autoscaler reconciliation cycle."
    )
    add_config_args(plan)
    plan.add_argument("--project", help="UCloud project id.")
    plan.add_argument(
        "--pending-vcpu",
        type=float,
        default=0.0,
        help="Total pending vCPU demand for unscheduled sandboxes.",
    )
    plan.add_argument(
        "--pending-memory-mb",
        type=int,
        default=0,
        help="Total pending memory demand in MB.",
    )
    plan.add_argument(
        "--pending-disk-mb",
        type=int,
        default=0,
        help="Total pending disk demand in MB.",
    )
    plan.add_argument(
        "--oldest-pending-seconds",
        type=int,
        default=0,
        help="Age of the oldest unscheduled sandbox request, for policy reporting.",
    )
    plan.add_argument(
        "--heartbeats",
        type=Path,
        help="Optional node heartbeat JSON file produced by VM node agents.",
    )
    plan.add_argument(
        "--jobs-file",
        type=Path,
        help="Optional UCloud jobs JSON fixture. If omitted, live UCloud jobs are browsed.",
    )
    plan.add_argument(
        "--include-job",
        action="append",
        default=[],
        help="Explicit job id to include even if it does not match the name prefix.",
    )
    plan.add_argument(
        "--all-vm-jobs",
        action="store_true",
        help="Treat every VM job in the project as part of the observed pool.",
    )
    plan.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    plan.set_defaults(func=cmd_plan)

    reconcile = subparsers.add_parser(
        "reconcile",
        help="Plan one autoscaler cycle and optionally execute VM mutations.",
    )
    add_config_args(reconcile)
    reconcile.add_argument("--project", help="UCloud project id.")
    reconcile.add_argument(
        "--pending-vcpu",
        type=float,
        default=0.0,
        help="Total pending vCPU demand for unscheduled sandboxes.",
    )
    reconcile.add_argument(
        "--pending-memory-mb",
        type=int,
        default=0,
        help="Total pending memory demand in MB.",
    )
    reconcile.add_argument(
        "--pending-disk-mb",
        type=int,
        default=0,
        help="Total pending disk demand in MB.",
    )
    reconcile.add_argument(
        "--oldest-pending-seconds",
        type=int,
        default=0,
        help="Age of the oldest unscheduled sandbox request.",
    )
    add_builder_autoscale_args(reconcile)
    add_vm_bootstrap_args(reconcile)
    reconcile.add_argument(
        "--heartbeats",
        type=Path,
        help="Optional node heartbeat JSON file produced by VM node agents.",
    )
    reconcile.add_argument(
        "--jobs-file",
        type=Path,
        help="Optional UCloud jobs JSON fixture. If omitted, live UCloud jobs are browsed.",
    )
    reconcile.add_argument(
        "--include-job",
        action="append",
        default=[],
        help="Explicit job id to include even if it does not match the name prefix.",
    )
    reconcile.add_argument(
        "--all-vm-jobs",
        action="store_true",
        help="Treat every VM job in the project as part of the observed pool.",
    )
    reconcile.add_argument(
        "--seed-prefix",
        help="Seed prefix for planned VM names. Defaults to a random cycle id.",
    )
    reconcile.add_argument(
        "--private-network-id",
        help="UCloud private network id. Defaults to config.private_network_id.",
    )
    reconcile.add_argument(
        "--no-private-network",
        action="store_true",
        help="Submit planned VM jobs without private-network attachment.",
    )
    reconcile.add_argument(
        "--app-name",
        default=DEFAULT_VM_APPLICATION_NAME,
        help="UCloud VM application name.",
    )
    reconcile.add_argument(
        "--app-version",
        default=DEFAULT_VM_APPLICATION_VERSION,
        help="UCloud VM application version.",
    )
    reconcile.add_argument(
        "--product-id",
        default=DEFAULT_VM_PRODUCT_ID,
        help="UCloud VM product id.",
    )
    reconcile.add_argument(
        "--product-category",
        default=DEFAULT_VM_PRODUCT_CATEGORY,
        help="UCloud VM product category.",
    )
    reconcile.add_argument(
        "--product-provider",
        default=DEFAULT_VM_PRODUCT_PROVIDER,
        help="UCloud VM product provider.",
    )
    reconcile.add_argument(
        "--disk-gb",
        type=int,
        default=DEFAULT_VM_DISK_GB,
        help="VM disk size parameter in GB.",
    )
    reconcile.add_argument(
        "--time-hours",
        type=int,
        default=1,
        help="VM time allocation hours.",
    )
    reconcile.add_argument(
        "--time-minutes",
        type=int,
        default=0,
        help="VM time allocation minutes.",
    )
    reconcile.add_argument(
        "--time-seconds",
        type=int,
        default=0,
        help="VM time allocation seconds.",
    )
    reconcile.add_argument(
        "--ssh",
        action="store_true",
        help=(
            "Request sshEnabled=true. The current vm-ubuntu:24.04 app rejects this "
            "on the live API."
        ),
    )
    reconcile.add_argument(
        "--no-ssh",
        action="store_true",
        help="Submit planned VM jobs without sshEnabled=true. This is the default.",
    )
    reconcile.add_argument(
        "--allow-duplicate-job",
        action="store_true",
        help="Allow UCloud to submit even when it detects duplicate jobs.",
    )
    reconcile.add_argument(
        "--label",
        action="append",
        default=[],
        help="UCloud job label as key=value. Repeat for multiple labels.",
    )
    reconcile.add_argument(
        "--execute",
        action="store_true",
        help="Submit planned create jobs. Default is dry-run.",
    )
    reconcile.add_argument(
        "--execute-stops",
        action="store_true",
        help="Terminate planned stop jobs. This is separate because it is destructive.",
    )
    reconcile.add_argument(
        "--allow-unlabeled-stops",
        action="store_true",
        help=(
            "Allow terminate requests for stop candidates without the matching "
            "deployment label. Unsafe; intended only for manual cleanup."
        ),
    )
    reconcile.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    reconcile.set_defaults(func=cmd_reconcile)

    loop = subparsers.add_parser(
        "autoscaler-loop",
        help="Run the autoscaler reconcile loop continuously on a gateway/control VM.",
    )
    add_config_args(loop)
    loop.add_argument("--project", help="UCloud project id.")
    loop.add_argument(
        "--interval-seconds",
        type=float,
        default=5.0,
        help="Delay between reconcile cycles.",
    )
    loop.add_argument(
        "--once",
        action="store_true",
        help="Run one loop cycle and exit.",
    )
    loop.add_argument(
        "--route-file",
        type=Path,
        help=(
            "Gateway route recovery and pending-demand database. "
            "Defaults to <state_dir>/routes.sqlite."
        ),
    )
    loop.add_argument(
        "--metrics-file",
        type=Path,
        help="JSONL metrics event file. Defaults to <state_dir>/metrics.jsonl.",
    )
    loop.add_argument(
        "--heartbeats",
        type=Path,
        help="Node heartbeat JSON file. Defaults to <state_dir>/heartbeats.json.",
    )
    loop.add_argument(
        "--jobs-file",
        type=Path,
        help="Optional UCloud jobs JSON fixture. If omitted, live UCloud jobs are browsed.",
    )
    loop.add_argument(
        "--include-job",
        action="append",
        default=[],
        help="Explicit job id to include even if it does not match the name prefix.",
    )
    loop.add_argument(
        "--all-vm-jobs",
        action="store_true",
        help="Treat every VM job in the project as part of the observed pool.",
    )
    loop.add_argument(
        "--seed-prefix",
        help="Seed prefix for planned VM names. Defaults to a random cycle id per cycle.",
    )
    loop.add_argument(
        "--private-network-id",
        help="UCloud private network id. Defaults to config.private_network_id.",
    )
    loop.add_argument(
        "--no-private-network",
        action="store_true",
        help="Submit planned VM jobs without private-network attachment.",
    )
    loop.add_argument("--app-name", default=DEFAULT_VM_APPLICATION_NAME)
    loop.add_argument("--app-version", default=DEFAULT_VM_APPLICATION_VERSION)
    loop.add_argument("--product-id", default=DEFAULT_VM_PRODUCT_ID)
    loop.add_argument("--product-category", default=DEFAULT_VM_PRODUCT_CATEGORY)
    loop.add_argument("--product-provider", default=DEFAULT_VM_PRODUCT_PROVIDER)
    loop.add_argument("--disk-gb", type=int, default=DEFAULT_VM_DISK_GB)
    add_builder_autoscale_args(loop)
    add_vm_bootstrap_args(loop)
    loop.add_argument("--time-hours", type=int, default=1)
    loop.add_argument("--time-minutes", type=int, default=0)
    loop.add_argument("--time-seconds", type=int, default=0)
    loop.add_argument("--ssh", action="store_true")
    loop.add_argument("--no-ssh", action="store_true")
    loop.add_argument("--allow-duplicate-job", action="store_true")
    loop.add_argument(
        "--label",
        action="append",
        default=[],
        help="UCloud job label as key=value. Repeat for multiple labels.",
    )
    loop.add_argument(
        "--execute",
        action="store_true",
        help="Submit planned create jobs. Default is dry-run.",
    )
    loop.add_argument(
        "--execute-stops",
        action="store_true",
        help="Terminate planned stop jobs. This is separate because it is destructive.",
    )
    loop.add_argument(
        "--allow-unlabeled-stops",
        action="store_true",
        help="Allow terminate requests for stop candidates without matching deployment label.",
    )
    loop.add_argument(
        "--output", choices=("text", "json"), default="text", help="Output format."
    )
    loop.set_defaults(func=cmd_autoscaler_loop)
    return parser


def add_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", type=Path, help="JSON autoscaler config file.")
    parser.add_argument(
        "--deployment-id",
        help="Deployment identity used in node heartbeats and UCloud job labels.",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        help="Autoscaler state directory. Defaults to the platform state directory.",
    )
    parser.add_argument(
        "--session-file",
        type=Path,
        help="UCloud CLI session file. Defaults to the ucloud-cli session path.",
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
    parser.add_argument(
        "--cpu-overcommit",
        type=float,
        default=1.0,
        help="CPU overcommit multiplier used for scheduling/accounting.",
    )
    parser.add_argument(
        "--memory-overcommit",
        type=float,
        default=1.0,
        help="Memory overcommit multiplier used for scheduling/accounting.",
    )
    parser.add_argument(
        "--disk-overcommit",
        type=float,
        default=1.0,
        help="Disk overcommit multiplier used for scheduling/accounting.",
    )


def add_builder_autoscale_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--scale-down-idle-seconds",
        type=int,
        default=None,
        help=(
            "Idle grace before stopping sandbox VMs after they become idle. "
            "Defaults to policy.scale_down_idle_seconds."
        ),
    )
    parser.add_argument(
        "--pending-image-builds",
        type=int,
        default=0,
        help="Pending image build requests needing builder capacity.",
    )
    parser.add_argument(
        "--max-builder-nodes",
        type=int,
        default=1,
        help="Maximum autoscaled builder VMs. Use 0 to disable builder creation.",
    )
    parser.add_argument(
        "--builder-product-id",
        default=DEFAULT_BUILDER_PRODUCT_ID,
        help="UCloud product id used for autoscaled builder VMs.",
    )
    parser.add_argument(
        "--builder-disk-gb",
        type=int,
        default=DEFAULT_BUILDER_DISK_GB,
        help="Disk size parameter in GB for autoscaled builder VMs.",
    )
    parser.add_argument(
        "--builder-scale-down-idle-seconds",
        type=int,
        default=None,
        help=(
            "Idle grace before stopping builder VMs after image-build demand "
            "drops to zero. Defaults to policy.builder_scale_down_idle_seconds."
        ),
    )


def add_vm_bootstrap_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--execute-init",
        action="store_true",
        help="Run post-boot init over SSH for eligible RUNNING autoscaled VMs.",
    )
    parser.add_argument(
        "--init-state-file",
        type=Path,
        help="VM init attempt state file. Defaults to <state_dir>/vm-bootstrap.json.",
    )
    parser.add_argument(
        "--max-init-per-cycle",
        type=int,
        default=1,
        help="Maximum VM init attempts per reconcile cycle.",
    )
    parser.add_argument(
        "--init-retry-seconds",
        type=int,
        default=30,
        help="Minimum delay before retrying VM init for the same job.",
    )
    parser.add_argument(
        "--init-timeout-seconds",
        type=int,
        default=1800,
        help="Timeout for one remote VM init attempt.",
    )
    parser.add_argument(
        "--init-heartbeat-url",
        default="",
        help="Heartbeat endpoint installed into autoscaled VMs.",
    )
    parser.add_argument(
        "--init-heartbeat-bearer-token-file",
        default="",
        help="Node-local token file used by the heartbeat service.",
    )
    parser.add_argument(
        "--init-heartbeat-bearer-token-source-file",
        type=Path,
        help=(
            "Gateway-local token file whose contents are installed on the node at "
            "--init-heartbeat-bearer-token-file."
        ),
    )
    parser.add_argument(
        "--init-service-user",
        default="ucloud",
        help="Linux user that owns autoscaled node services.",
    )
    parser.add_argument(
        "--init-authorized-key",
        action="append",
        default=[],
        help="SSH public key installed for the service user during autoscaled VM init.",
    )
    parser.add_argument(
        "--init-authorized-key-file",
        action="append",
        default=[],
        type=Path,
        help="Read SSH public keys installed during autoscaled VM init.",
    )
    parser.add_argument(
        "--init-work-dir",
        default="/work/ucloud-sandboxes",
        help="Persistent node work directory for autoscaled VM init.",
    )
    parser.add_argument(
        "--init-package-spec",
        default="ucloud-sandboxes",
        help="pip package spec installed into autoscaled VMs.",
    )
    parser.add_argument(
        "--init-node-agent-host",
        default="0.0.0.0",
        help="Bind address for autoscaled node agents.",
    )
    parser.add_argument(
        "--init-node-agent-port",
        type=int,
        default=8090,
        help="Node-agent port for autoscaled VMs.",
    )
    parser.add_argument(
        "--init-ssh-port-start",
        type=int,
        default=22000,
        help="First host port for per-sandbox SSH on autoscaled VMs.",
    )
    parser.add_argument(
        "--init-ssh-port-end",
        type=int,
        default=22999,
        help="Last host port for per-sandbox SSH on autoscaled VMs.",
    )
    parser.add_argument(
        "--init-heartbeat-interval-seconds",
        type=int,
        default=20,
        help="systemd timer interval for autoscaled node heartbeats.",
    )
    parser.add_argument(
        "--init-docker-quota-image-gb",
        type=int,
        default=DEFAULT_DOCKER_QUOTA_IMAGE_GB,
        help="Sparse XFS image size in GB for autoscaled VM Docker quotas.",
    )
    parser.add_argument(
        "--init-docker-insecure-registry",
        action="append",
        default=[],
        help=(
            "Docker registry host[:port] trusted as HTTP/insecure on autoscaled "
            "VMs. Repeat for multiple private registries."
        ),
    )
    parser.add_argument(
        "--init-host-alias",
        action="append",
        default=[],
        metavar="HOST=ADDRESS",
        help=(
            "Add an /etc/hosts entry during autoscaled VM init. Use this for "
            "stable private service names such as ucloud-sandbox-registry."
        ),
    )
    parser.add_argument(
        "--init-cpu-overcommit",
        type=float,
        default=1.0,
        help="CPU overcommit multiplier advertised by autoscaled VM node agents.",
    )
    parser.add_argument(
        "--init-memory-overcommit",
        type=float,
        default=1.0,
        help="Memory overcommit multiplier advertised by autoscaled VM node agents.",
    )
    parser.add_argument(
        "--init-disk-overcommit",
        type=float,
        default=1.0,
        help="Disk overcommit multiplier advertised by autoscaled VM node agents.",
    )
    parser.add_argument(
        "--init-runtime-dry-run",
        action="store_true",
        help="Initialize node-agent without --execute-runtime.",
    )
    parser.add_argument(
        "--init-ssh-private-key-file",
        help="Private key file passed to ssh for autoscaled VM init.",
    )


def add_vm_init_args(
    parser: argparse.ArgumentParser,
    *,
    include_job_id: bool,
) -> None:
    if include_job_id:
        parser.add_argument("--job-id", required=True, help="UCloud VM job id.")
    parser.add_argument(
        "--heartbeat-url",
        required=True,
        help="Control-plane heartbeat URL, e.g. https://.../v1/nodes/heartbeat.",
    )
    parser.add_argument(
        "--heartbeat-bearer-token-file",
        default="",
        help="Token file on the VM used to authenticate heartbeat posts.",
    )
    parser.add_argument(
        "--heartbeat-bearer-token-source-file",
        type=Path,
        help=(
            "Local token file whose contents are installed on the VM at "
            "--heartbeat-bearer-token-file."
        ),
    )
    parser.add_argument(
        "--service-user",
        default="ucloud",
        help="Linux user that owns the venv/state and runs node services.",
    )
    parser.add_argument(
        "--init-authorized-key",
        action="append",
        default=[],
        help="SSH public key installed for the service user during VM init. Repeatable.",
    )
    parser.add_argument(
        "--init-authorized-key-file",
        action="append",
        default=[],
        type=Path,
        help="Read SSH public keys to install during VM init, one key per line.",
    )
    parser.add_argument(
        "--node-id",
        help="Stable node id. Defaults to ucloud-vm-<job-id>.",
    )
    parser.add_argument(
        "--work-dir",
        default="/work/ucloud-sandboxes",
        help="Persistent VM work directory for state, caches, and venv.",
    )
    parser.add_argument(
        "--package-spec",
        default="ucloud-sandboxes",
        help="pip package spec installed into the VM venv.",
    )
    parser.add_argument(
        "--node-agent-host",
        default="0.0.0.0",
        help="Bind address for the VM-side node agent.",
    )
    parser.add_argument(
        "--node-agent-port",
        type=int,
        default=8090,
        help="Local node-agent HTTP port on the VM.",
    )
    parser.add_argument(
        "--node-url",
        help=(
            "Node-agent URL advertised in heartbeats. Defaults to "
            "http://<node-id>:<node-agent-port>."
        ),
    )
    parser.add_argument(
        "--ssh-port-start",
        type=int,
        default=22000,
        help="First localhost port for per-sandbox SSH.",
    )
    parser.add_argument(
        "--ssh-port-end",
        type=int,
        default=22999,
        help="Last localhost port for per-sandbox SSH.",
    )
    parser.add_argument(
        "--heartbeat-interval-seconds",
        type=int,
        default=20,
        help="systemd timer interval for node heartbeats.",
    )
    parser.add_argument(
        "--docker-quota-image-gb",
        type=int,
        default=DEFAULT_DOCKER_QUOTA_IMAGE_GB,
        help=(
            "Sparse XFS image size in GB for Docker overlay2 project quotas. "
            "Use 0 to disable quota-backed Docker storage."
        ),
    )
    parser.add_argument(
        "--docker-insecure-registry",
        action="append",
        default=[],
        help=(
            "Docker registry host[:port] trusted as HTTP/insecure on this VM. "
            "Repeat for multiple private registries."
        ),
    )
    parser.add_argument(
        "--host-alias",
        action="append",
        default=[],
        metavar="HOST=ADDRESS",
        help=(
            "Add an /etc/hosts entry during VM init. Use this for stable "
            "private service names such as ucloud-sandbox-registry."
        ),
    )
    add_node_version_args(parser)
    add_resource_args(parser)
    parser.add_argument(
        "--enable-image-builds",
        action="store_true",
        help=(
            "Configure this VM as a builder-only node-agent. Production builds "
            "should run on the control plane."
        ),
    )
    parser.add_argument(
        "--runtime-dry-run",
        action="store_true",
        help="Start node-agent without --execute-runtime.",
    )
    parser.add_argument(
        "--label",
        action="append",
        default=[],
        help="Node heartbeat label as key=value. Repeat for multiple labels.",
    )


def load_config(args: argparse.Namespace) -> AutoscalerConfig:
    config = (
        AutoscalerConfig.from_file(args.config)
        if getattr(args, "config", None)
        else AutoscalerConfig.default()
    )
    if getattr(args, "session_file", None):
        config = AutoscalerConfig(
            project_id=config.project_id,
            deployment_id=config.deployment_id,
            job_name_prefix=config.job_name_prefix,
            template_job_id=config.template_job_id,
            private_network_id=config.private_network_id,
            gateway_public_link_id=config.gateway_public_link_id,
            gateway_public_link_port=config.gateway_public_link_port,
            node_hostname_prefix=config.node_hostname_prefix,
            ucloud_session_file=str(args.session_file),
            state_dir=config.state_dir,
            metrics_file=config.metrics_file,
            policy=config.policy,
        )
    if getattr(args, "deployment_id", None):
        config = AutoscalerConfig(
            project_id=config.project_id,
            deployment_id=str(args.deployment_id),
            job_name_prefix=config.job_name_prefix,
            template_job_id=config.template_job_id,
            private_network_id=config.private_network_id,
            gateway_public_link_id=config.gateway_public_link_id,
            gateway_public_link_port=config.gateway_public_link_port,
            node_hostname_prefix=config.node_hostname_prefix,
            ucloud_session_file=config.ucloud_session_file,
            state_dir=config.state_dir,
            metrics_file=config.metrics_file,
            policy=config.policy,
        )
    if getattr(args, "state_dir", None):
        config = config.with_state_dir(str(args.state_dir))
    return config


def cmd_sample_config(_args: argparse.Namespace) -> int:
    print(json.dumps(AutoscalerConfig.default().to_dict(), indent=2, sort_keys=True))
    return 0


def cmd_inspect_job(args: argparse.Namespace) -> int:
    config = load_config(args).with_project_id(args.project)
    if not config.project_id:
        raise ValueError("project id is required via --project or config.project_id.")
    client = UCloudClient(SessionStore(Path(config.ucloud_session_file)))
    payload = client.retrieve_job(config.project_id, args.job_id)
    job = vm_job_from_payload(payload)
    if args.output == "json":
        print_json(vm_job_to_dict(job))
    else:
        print_vm_job(job)
    return 0


def cmd_agent_heartbeat(args: argparse.Namespace) -> int:
    config = load_config(args)
    labels = parse_labels(args.label)
    if args.from_node_agent_url:
        heartbeat = fetch_node_agent_heartbeat(args.from_node_agent_url)
        if config.deployment_id and not heartbeat.deployment_id:
            heartbeat = replace(heartbeat, deployment_id=config.deployment_id)
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
            deployment_id=config.deployment_id,
            init_version=args.init_version,
            capabilities=merge_capabilities(
                tuple(args.capability),
                conformance_capabilities_from_file(
                    getattr(args, "runtime_conformance_file", None)
                ),
            ),
            total_resources=resource_quantity_from_args(args),
            used_resources=ResourceQuantity(),
            cpu_overcommit=args.cpu_overcommit,
            memory_overcommit=args.memory_overcommit,
            disk_overcommit=args.disk_overcommit,
            labels=labels,
        )

    result: dict[str, Any] = {"heartbeat": heartbeat_to_dict(heartbeat)}
    if args.heartbeat_file:
        store = HeartbeatStore(args.heartbeat_file)
        store.upsert(heartbeat)
        result["heartbeatFile"] = str(args.heartbeat_file)
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
        printable = dict(result)
        for key in ("rawNodes", "rawDecision", "rawCreateIntents"):
            printable.pop(key, None)
        print_json(printable)
    else:
        print(f"Heartbeat: node={heartbeat.node_id} job={heartbeat.job_id}")
        if heartbeat.node_url:
            print(f"Node URL: {heartbeat.node_url}")
        print(f"Active: {heartbeat.active_sandboxes}, draining: {heartbeat.draining}")
        if args.heartbeat_file:
            print(f"Wrote: {args.heartbeat_file}")
        if args.post_url:
            print(f"Posted: {args.post_url}")
        if not args.heartbeat_file and not args.post_url:
            print_json(heartbeat_to_dict(heartbeat))
    del config
    return 0


def cmd_serve_control_plane(args: argparse.Namespace) -> int:
    config = load_config(args)
    heartbeat_file = args.heartbeat_file or config.heartbeat_file()
    route_file = args.route_file or config.routing_file()
    metrics_file = metrics_path_from_args(args, config, sibling_file=route_file)
    gateway_bearer_token = None
    if args.gateway_bearer_token_file:
        gateway_bearer_token = args.gateway_bearer_token_file.read_text(
            encoding="utf-8"
        ).strip()
        if not gateway_bearer_token:
            raise ValueError("gateway bearer token file is empty.")
    registry_url = (
        args.registry_url
        or os.environ.get("UCLOUD_SANDBOX_REGISTRY_URL")
        or os.environ.get("UCLOUD_REGISTRY_URL")
    )
    server = build_server(
        args.host,
        args.port,
        heartbeat_file,
        routing_file=route_file,
        upstream_node_url=args.gateway_upstream_node_url,
        gateway_bearer_token=gateway_bearer_token,
        heartbeat_ttl_seconds=(
            args.heartbeat_ttl_seconds
            if args.heartbeat_ttl_seconds is not None
            else config.policy.heartbeat_ttl_seconds
        ),
        image_file=args.image_file or config.image_file(),
        image_runtime=(
            DockerImageRuntime(
                docker_binary=args.docker_binary,
                dry_run=not args.execute_image_builds,
            )
            if args.enable_image_builds
            else None
        ),
        local_image_builds_enabled=args.enable_image_builds,
        metrics_file=metrics_file,
        registry_url=registry_url,
        max_concurrent_sandbox_creates=args.max_concurrent_sandbox_creates,
    )
    host, port = server.server_address
    print(f"Serving heartbeat receiver on http://{host}:{port}")
    print(f"Heartbeat file: {heartbeat_file}")
    print(f"Route file: {route_file}")
    print(f"Metrics file: {metrics_file}")
    if args.gateway_upstream_node_url:
        print(f"Gateway upstream node: {args.gateway_upstream_node_url}")
    if gateway_bearer_token:
        print("Gateway auth: bearer token required")
    if registry_url:
        print(f"Registry metrics: {registry_url}")
    print(
        "Image builds: "
        + (
            "execute"
            if args.enable_image_builds and args.execute_image_builds
            else "dry-run"
        )
        if args.enable_image_builds
        else "Image builds: disabled"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping heartbeat receiver.")
    finally:
        server.server_close()
    return 0


def cmd_serve_node_agent(args: argparse.Namespace) -> int:
    config = load_config(args)
    sandbox_file = args.sandbox_file or config.sandbox_file()
    image_file = args.image_file or config.image_file()
    job_id = args.job_id or detect_job_id()
    if not job_id:
        raise ValueError("job id is required via --job-id or UCLOUD_JOB_ID.")
    node_id = args.node_id or default_node_id(job_id)
    runtime_conformance_file = getattr(args, "runtime_conformance_file", None)
    conformance_capabilities = conformance_capabilities_from_file(
        runtime_conformance_file
    )
    conformance_results = conformance_results_from_file(runtime_conformance_file)
    runtime = DockerGvisorRuntime(
        docker_binary=args.docker_binary,
        runtime_name=args.runtime_name,
        allow_storage_opt_quota=has_capability(
            conformance_capabilities,
            DISK_QUOTA_CAPABILITY,
        ),
        allow_tmpfs_workspace=bool(conformance_results.get(TMPFS_QUOTA_PROBE)),
        dry_run=not args.execute_runtime,
    )
    image_runtime = DockerImageRuntime(
        docker_binary=args.docker_binary,
        dry_run=not args.execute_runtime,
    )
    server = build_node_agent_server(
        args.host,
        args.port,
        sandbox_file=sandbox_file,
        image_file=image_file,
        job_id=job_id,
        node_id=node_id,
        node_url=args.node_url,
        agent_version=args.agent_version,
        deployment_id=config.deployment_id,
        init_version=args.init_version,
        total_resources=resource_quantity_from_args(args),
        cpu_overcommit=args.cpu_overcommit,
        memory_overcommit=args.memory_overcommit,
        disk_overcommit=args.disk_overcommit,
        extra_capabilities=conformance_capabilities,
        runtime=runtime,
        image_runtime=image_runtime,
        ssh_port_range=(args.ssh_port_start, args.ssh_port_end),
        image_builds_enabled=args.enable_image_builds,
    )
    host, port = server.server_address
    mode = "execute" if args.execute_runtime else "dry-run"
    print(f"Serving node agent on http://{host}:{port}")
    print(f"Runtime mode: {mode}")
    print(f"Sandbox file: {sandbox_file}")
    print(f"Image file: {image_file}")
    print(f"Node URL: {args.node_url or ''}")
    print(f"Deployment: {config.deployment_id}")
    print(f"Agent version: {args.agent_version}")
    print(f"Init version: {args.init_version}")
    print(f"SSH port range: {args.ssh_port_start}-{args.ssh_port_end}")
    print(f"Image builds: {'enabled' if args.enable_image_builds else 'disabled'}")
    print(
        "Resources: "
        f"{args.total_vcpu} vCPU, {args.total_memory_mb} MB RAM, "
        f"{args.total_disk_mb} MB disk "
        f"(overcommit cpu={args.cpu_overcommit}, memory={args.memory_overcommit}, "
        f"disk={args.disk_overcommit})"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping node agent.")
    finally:
        server.server_close()
    return 0


def cmd_serve_async_node_agent(args: argparse.Namespace) -> int:
    from aiohttp import web

    config = load_config(args)
    sandbox_file = args.sandbox_file or config.sandbox_file()
    image_file = args.image_file or config.image_file()
    runtime = DockerGvisorRuntime(
        docker_binary=args.docker_binary,
        runtime_name=args.runtime_name,
        dry_run=not args.execute_runtime,
    )
    app = create_async_node_agent_app(
        sandbox_file=sandbox_file,
        image_file=image_file,
        runtime=runtime,
        ssh_port_range=(args.ssh_port_start, args.ssh_port_end),
    )
    mode = "execute" if args.execute_runtime else "dry-run"
    print(f"Serving async node agent on http://{args.host}:{args.port}")
    print(f"Runtime mode: {mode}")
    print(f"Sandbox file: {sandbox_file}")
    print(f"Image file: {image_file}")
    print(f"SSH port range: {args.ssh_port_start}-{args.ssh_port_end}")
    web.run_app(app, host=args.host, port=args.port, print=None)
    return 0


def cmd_serve_model_relay(args: argparse.Namespace) -> int:
    from aiohttp import web

    sandbox_bearer_token = read_required_token_file(
        args.sandbox_bearer_token_file,
        "sandbox bearer token",
    )
    worker_bearer_token = read_required_token_file(
        args.worker_bearer_token_file,
        "worker bearer token",
    )
    app = create_model_relay_app(
        sandbox_bearer_token=sandbox_bearer_token,
        worker_bearer_token=worker_bearer_token,
        request_timeout_seconds=max(0.1, args.request_timeout_seconds),
        worker_poll_timeout_seconds=max(0.0, args.worker_poll_timeout_seconds),
        worker_lease_seconds=max(0.001, args.worker_lease_seconds),
        completed_request_retention_seconds=max(
            1.0,
            args.completed_request_retention_seconds,
        ),
    )
    print(f"Serving model relay on http://{args.host}:{args.port}")
    print(f"Sandbox auth: {'required' if sandbox_bearer_token else 'disabled'}")
    print(f"Worker auth: {'required' if worker_bearer_token else 'disabled'}")
    print(f"Request timeout: {max(0.1, args.request_timeout_seconds):g}s")
    print(f"Worker lease: {max(0.001, args.worker_lease_seconds):g}s")
    web.run_app(app, host=args.host, port=args.port, print=None)
    return 0


def cmd_render_vm_init_script(args: argparse.Namespace) -> int:
    options = vm_init_options_from_args(args, args.job_id)
    script = render_vm_init_script(options)
    if args.output == "json":
        print_json({"script": script, "options": vm_init_options_to_dict(options)})
    else:
        print(script, end="" if script.endswith("\n") else "\n")
    return 0


def cmd_init_vm(args: argparse.Namespace) -> int:
    config = load_config(args).with_project_id(args.project)
    if not config.project_id:
        raise ValueError("project id is required via --project or config.project_id.")

    client = UCloudClient(SessionStore(Path(config.ucloud_session_file)))
    payload = client.retrieve_job(config.project_id, args.job_id, include_updates=True)
    plan = plan_vm_init(payload)
    options = vm_init_options_from_args(args, args.job_id)
    script = render_vm_init_script(options)

    if args.output == "script":
        print(script, end="" if script.endswith("\n") else "\n")
        return 0

    result: dict[str, Any] = {
        "projectId": config.project_id,
        "job": vm_job_to_dict(plan.job),
        "sshCommand": plan.ssh_command,
        "runnable": plan.runnable,
        "reason": plan.reason,
        "options": vm_init_options_to_dict(options),
        "execute": args.execute,
    }

    if args.execute:
        if not plan.runnable or not plan.ssh_command:
            raise ValueError(plan.reason)
        effective_options = options
        stage_result = stage_vm_init_package_over_ssh(
            plan.ssh_command,
            options,
            timeout_seconds=max(1, args.timeout_seconds),
            private_key_file=args.ssh_private_key_file,
        )
        if stage_result is not None:
            result["packageStage"] = {
                "localPath": str(stage_result.local_path),
                "remotePath": stage_result.remote_path,
                "command": list(stage_result.command),
                "returncode": stage_result.returncode,
            }
            if stage_result.returncode != 0:
                raise ValueError(
                    f"remote package staging failed with exit code {stage_result.returncode}"
                )
            effective_options = replace(options, package_spec=stage_result.remote_path)
        run_result = run_init_over_ssh(
            plan.ssh_command,
            render_vm_init_script(effective_options),
            timeout_seconds=max(1, args.timeout_seconds),
            private_key_file=args.ssh_private_key_file,
        )
        result["run"] = {
            "command": list(run_result.command),
            "returncode": run_result.returncode,
        }
        if run_result.returncode != 0:
            raise ValueError(
                f"remote init failed with exit code {run_result.returncode}"
            )

    if args.output == "json":
        print_json(result)
    else:
        print(f"Project: {config.project_id}")
        print(f"Job: {plan.job.id}")
        print(f"State: {plan.job.state}")
        print(f"SSH enabled: {plan.job.ssh_enabled}")
        print(f"SSH command: {plan.ssh_command or ''}")
        print(f"Deployment: {options.deployment_id}")
        print(f"Agent version: {options.agent_version}")
        print(f"Init version: {options.init_version}")
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
    client = UCloudClient(SessionStore(Path(config.ucloud_session_file)))

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


def cmd_runtime_conformance(args: argparse.Namespace) -> int:
    report = DockerRuntimeProbe(
        docker_binary=args.docker_binary,
        runtime_name=args.runtime_name,
        image=args.image,
        use_sudo=args.sudo,
        execute=args.execute,
    ).run()
    payload = report.to_dict()
    if args.output == "json":
        print_json(payload)
        return 0 if report.ok else 1

    print(f"Runtime: {report.runtime_name}")
    print(f"Image: {report.image}")
    print(f"Mode: {'execute' if report.executed else 'dry-run'}")
    print(f"Overall: {'ok' if report.ok else 'failed'}")
    for result in report.results:
        if result.skipped:
            status = "skipped"
        else:
            status = "ok" if result.ok else "failed"
        print(f"- {result.name}: {status}")
        print(f"  command: {' '.join(result.command)}")
        if result.detail:
            print(f"  detail: {result.detail}")
        if result.exit_code is not None:
            print(f"  exit: {result.exit_code}")
    return 0 if report.ok else 1


def cmd_vm_network_attachment(args: argparse.Namespace) -> int:
    config = load_config(args)
    private_network_id = args.private_network_id or config.private_network_id
    if not private_network_id:
        raise ValueError(
            "private network id is required via --private-network-id or config."
        )
    hostname_prefix = args.hostname_prefix or config.node_hostname_prefix
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
    public_link_id = args.public_link_id or config.gateway_public_link_id
    if not public_link_id:
        raise ValueError(
            "public link id is required via --public-link-id or "
            "config.gateway_public_link_id."
        )
    port = (
        args.port
        if args.port is not None
        else config.gateway_public_link_port or DEFAULT_PUBLIC_LINK_PORT
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
    if args.keep_per_repository < 0:
        raise ValueError("keep-per-repository cannot be negative.")
    client = RegistryClient(args.registry_url)
    plan = registry_prune_plan(
        client,
        keep_per_repository=args.keep_per_repository,
        repository_prefix=args.repository_prefix,
    )
    plan["execute"] = bool(args.execute)
    if args.execute:
        records = list_registry_tags(
            client,
            repository_prefix=args.repository_prefix,
        )
        candidates = select_prune_candidates(
            records,
            keep_per_repository=args.keep_per_repository,
        )
        deleted = execute_registry_prune(client, candidates)
        plan["deleted"] = [item.to_dict() for item in deleted]
    print_json(plan)
    return 0


def cmd_submit_vm(args: argparse.Namespace) -> int:
    config = load_config(args).with_project_id(args.project)
    if not config.project_id:
        raise ValueError("project id is required via --project or config.project_id.")

    options, seed = vm_submission_options_from_args(args, config)
    payload = options.bulk_payload()
    result: dict[str, Any] = {
        "projectId": config.project_id,
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
        client = UCloudClient(SessionStore(Path(config.ucloud_session_file)))
        response = client.submit_jobs(config.project_id, payload)
        result["response"] = response
        job_ids = submitted_job_ids(response)
        result["jobIds"] = job_ids

    if args.output == "json":
        print_json(result)
    else:
        print(f"Project: {config.project_id}")
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
                    print(
                        "Next: "
                        f"ucloud-sandboxes init-vm {job_ids[0]} "
                        f"--project {config.project_id} "
                        f"--node-id {options.hostname} "
                        "--heartbeat-url <control-plane-url>/v1/nodes/heartbeat"
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
    config = load_config(args).with_project_id(args.project)
    if not config.project_id:
        raise ValueError("project id is required via --project or config.project_id.")
    if args.port < 1 or args.port > 65535:
        raise ValueError("port must be in [1, 65535].")
    if args.rank < 0:
        raise ValueError("rank cannot be negative.")

    client = UCloudClient(SessionStore(Path(config.ucloud_session_file)))
    response = client.open_interactive_session(
        config.project_id,
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
    config = load_config(args).with_project_id(args.project)
    if not config.project_id:
        raise ValueError("project id is required via --project or config.project_id.")
    if not config.deployment_id:
        raise ValueError("deployment id is required via --deployment-id or config.")
    private_network_id = args.private_network_id or config.private_network_id
    if not private_network_id:
        raise ValueError(
            "private network id is required via --private-network-id or config."
        )

    client: UCloudClient | None = None

    def get_client() -> UCloudClient:
        nonlocal client
        if client is None:
            client = UCloudClient(SessionStore(Path(config.ucloud_session_file)))
        return client

    payload: dict[str, Any] | None = None

    def get_payload() -> dict[str, Any]:
        nonlocal payload
        if payload is None:
            payload = get_client().retrieve_job(
                config.project_id,
                args.job_id,
                include_updates=True,
            )
        return payload

    ssh_command = args.ssh_command
    if not ssh_command and args.execute:
        init_plan = plan_vm_init(get_payload())
        if not init_plan.runnable or not init_plan.ssh_command:
            raise ValueError(init_plan.reason)
        ssh_command = init_plan.ssh_command

    inferred_job: VmJob | None = None
    if not args.gateway_private_host:
        inferred_job = vm_job_from_payload(get_payload())
    gateway_private_host = args.gateway_private_host or (
        inferred_job.hostname if inferred_job is not None else ""
    )
    registry_private_ip = args.registry_private_ip or ""

    plan = AllInOneDeployPlan(
        job_id=args.job_id,
        project_id=config.project_id,
        deployment_id=config.deployment_id,
        local_wheel=args.wheel.expanduser().resolve(),
        install_root=args.install_root,
        project_mount_dir=args.project_mount_dir,
        service_user=args.service_user,
        gateway_port=args.gateway_port,
        relay_port=args.relay_port,
        registry_port=args.registry_port,
        registry_alias=args.registry_alias,
        registry_private_ip=registry_private_ip,
        gateway_private_host=gateway_private_host,
        private_network_id=private_network_id,
        sandbox_product_id=args.sandbox_product_id,
        sandbox_disk_gb=args.sandbox_disk_gb,
        sandbox_idle_seconds=args.sandbox_idle_seconds,
        builder_product_id=args.builder_product_id,
        builder_disk_gb=args.builder_disk_gb,
        builder_idle_seconds=args.builder_idle_seconds,
        max_builder_nodes=args.max_builder_nodes,
        autoscaler_interval_seconds=args.autoscaler_interval_seconds,
        cpu_overcommit=args.cpu_overcommit,
        memory_overcommit=args.memory_overcommit,
        disk_overcommit=args.disk_overcommit,
        docker_quota_image_gb=args.docker_quota_image_gb,
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
        if not args.no_copy_session:
            local_session = Path(config.ucloud_session_file).expanduser()
            staged_session = stage_file_over_ssh(
                ssh_command,
                local_session,
                plan.remote_session_file,
                mode="0600",
                timeout_seconds=timeout,
                private_key_file=args.ssh_private_key_file,
            )
            result["stagedFiles"].append(
                {
                    "localPath": str(local_session),
                    "remotePath": plan.remote_session_file,
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
            for port in (plan.gateway_port, plan.relay_port):
                response = get_client().open_interactive_session(
                    config.project_id,
                    args.job_id,
                    session_type="WEB",
                    rank=0,
                    port=port,
                )
                result["openWeb"].append({"port": port, "response": response})

    if args.output == "json":
        print_json(result)
    else:
        print(f"Project: {config.project_id}")
        print(f"Job: {args.job_id}")
        print(f"Deployment: {config.deployment_id}")
        print(f"Version: {plan.package_version}")
        print(f"Wheel: {plan.local_wheel}")
        print(f"Remote wheel: {plan.remote_wheel_path}")
        print(f"Private gateway host: {plan.gateway_private_host}")
        print(f"Registry alias: {plan.docker_host_alias}")
        print(f"Mode: {'execute' if args.execute else 'dry-run'}")
        if args.execute:
            print(
                "Services converged: gateway, relay, registry, registry GC, autoscaler"
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
    heartbeat_file = args.heartbeat_file or config.heartbeat_file()
    heartbeats = load_heartbeats(heartbeat_file)
    nodes = [heartbeat_to_dict(heartbeats[job_id]) for job_id in sorted(heartbeats)]
    if args.output == "json":
        print_json({"heartbeatFile": str(heartbeat_file), "nodes": nodes})
    else:
        print(f"Heartbeat file: {heartbeat_file}")
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


def cmd_plan(args: argparse.Namespace) -> int:
    config = load_config(args).with_project_id(args.project)
    if not config.project_id:
        raise ValueError("project id is required via --project or config.project_id.")

    jobs = load_jobs_for_plan(config, args)
    heartbeat_file = args.heartbeats or config.heartbeat_file()
    heartbeats = load_heartbeats(heartbeat_file)
    nodes = merge_jobs_and_heartbeats(jobs, heartbeats, config.policy)
    decision = evaluate_scale(
        nodes,
        sandbox_demand_from_args(args),
        config.policy,
    )

    if args.output == "json":
        print_json(
            {
                "projectId": config.project_id,
                "jobNamePrefix": config.job_name_prefix,
                "heartbeatFile": str(heartbeat_file),
                "nodes": [node_to_dict(node) for node in nodes],
                "decision": scale_decision_to_dict(decision),
            }
        )
    else:
        print_plan(config, nodes, decision, heartbeat_file)
    return 0


def cmd_reconcile(args: argparse.Namespace) -> int:
    config = load_config(args).with_project_id(args.project)
    if not config.project_id:
        raise ValueError("project id is required via --project or config.project_id.")

    result = run_reconcile_cycle(
        config,
        args,
        demand=sandbox_demand_from_args(args),
    )

    if args.output == "json":
        printable = dict(result)
        for key in (
            "rawNodes",
            "rawSandboxNodes",
            "rawBuilderNodes",
            "rawDecision",
            "rawBuilderDecision",
            "rawCreateIntents",
            "rawSandboxCreateIntents",
            "rawBuilderCreateIntents",
            "rawBootstrapIntents",
        ):
            printable.pop(key, None)
        print_json(printable)
    else:
        print_reconcile(
            config,
            result["rawSandboxNodes"],
            result["rawDecision"],
            Path(result["heartbeatFile"]),
            result["rawCreateIntents"],
            tuple(result["stopJobIds"]),
            result,
        )
    return 0


def cmd_autoscaler_loop(args: argparse.Namespace) -> int:
    config = load_config(args).with_project_id(args.project)
    if not config.project_id:
        raise ValueError("project id is required via --project or config.project_id.")
    route_file = args.route_file or config.routing_file()
    metrics_file = metrics_path_from_args(args, config, sibling_file=route_file)
    metrics_store = MetricsStore(metrics_file)
    interval = max(1.0, float(args.interval_seconds))
    cycle = 0
    observed_vm_keys: dict[str, tuple[object, ...]] = {}
    while True:
        cycle += 1
        routing_store = RoutingStore(route_file)
        demand = routing_store.pending_demand()
        pending_image_builds = max(
            int(getattr(args, "pending_image_builds", 0) or 0),
            routing_store.pending_image_build_count(),
        )
        prepared_builder_count = routing_store.prepared_builder_count()
        result = run_reconcile_cycle(
            config,
            args,
            demand=demand,
            pending_image_builds=pending_image_builds,
            prepared_builder_count=prepared_builder_count,
            metrics_store=metrics_store,
        )
        route_cleanup_job_ids = set(result.get("prunedFinalHeartbeats", []))
        if result.get("stopResponse") is not None:
            route_cleanup_job_ids.update(str(job_id) for job_id in result["stopJobIds"])
        removed_routes = routing_store.delete_sandboxes_for_jobs(route_cleanup_job_ids)
        if args.execute:
            effective_policy = policy_with_cli_overrides(config.policy, args)
            stale_route_grace_seconds = max(
                effective_policy.heartbeat_ttl_seconds * 3,
                effective_policy.heartbeat_ttl_seconds + 60,
            )
            active_route_job_ids = {
                node.job_id for node in result["rawNodes"] if not node.job.is_final
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
                    older_than=utc_now() - timedelta(seconds=stale_route_grace_seconds),
                )
            )
        consumed_pending_demand = (
            routing_store.consume_pending_demand() if args.execute else []
        )
        consumed_prepared_capacity = (
            routing_store.consume_prepared_capacity() if args.execute else []
        )
        consumed_pending_image_builds = (
            routing_store.consume_pending_image_builds() if args.execute else []
        )
        consumed_prepared_builders = (
            routing_store.consume_prepared_builders() if args.execute else []
        )
        result["cycle"] = cycle
        result["routeFile"] = str(route_file)
        result["metricsFile"] = str(metrics_file)
        result["consumedPendingDemand"] = [
            item.to_dict() for item in consumed_pending_demand
        ]
        result["consumedPreparedCapacity"] = [
            item.to_dict() for item in consumed_prepared_capacity
        ]
        result["consumedPendingImageBuilds"] = [
            item.to_dict() for item in consumed_pending_image_builds
        ]
        result["consumedPreparedBuilders"] = [
            item.to_dict() for item in consumed_prepared_builders
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
                "rawSandboxCreateIntents",
                "rawBuilderCreateIntents",
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
                config,
                result["rawSandboxNodes"],
                result["rawDecision"],
                Path(result["heartbeatFile"]),
                result["rawCreateIntents"],
                tuple(result["stopJobIds"]),
                result,
            )
        sys.stdout.flush()
        if args.once:
            return 0
        time.sleep(interval)


def run_reconcile_cycle(
    config: AutoscalerConfig,
    args: argparse.Namespace,
    *,
    demand: SandboxDemand,
    pending_image_builds: int | None = None,
    prepared_builder_count: int | None = None,
    metrics_store: MetricsStore | None = None,
) -> dict[str, Any]:
    jobs = load_jobs_for_plan(config, args)
    heartbeat_file = args.heartbeats or config.heartbeat_file()
    heartbeat_store = HeartbeatStore(Path(heartbeat_file))
    heartbeats = load_heartbeats(heartbeat_file)
    final_heartbeat_job_ids = tuple(
        sorted(job.id for job in jobs if job.is_final and job.id in heartbeats)
    )
    if final_heartbeat_job_ids:
        heartbeat_store.remove(final_heartbeat_job_ids)
        heartbeats = {
            job_id: heartbeat
            for job_id, heartbeat in heartbeats.items()
            if job_id not in final_heartbeat_job_ids
        }
    effective_policy = policy_with_cli_overrides(config.policy, args)
    nodes = merge_jobs_and_heartbeats(jobs, heartbeats, effective_policy)
    sandbox_nodes = sandbox_pool_nodes(nodes, config)
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
    decision = evaluate_scale(sandbox_nodes, demand, effective_policy)
    builder_decision = evaluate_builder_scale(
        builder_nodes,
        pending_builds=builder_pending,
        prepared_builders=builder_prepared,
        policy=effective_policy,
        max_builder_nodes=getattr(args, "max_builder_nodes", 1),
    )
    sandbox_create_intents: list[VmCreateIntent] = []
    if decision.creates > 0:
        sandbox_create_intents = build_vm_create_intents(
            config,
            decision,
            vm_node_submission_defaults_from_args(args, config),
            seed_prefix=args.seed_prefix,
        )
    builder_create_intents: list[VmCreateIntent] = []
    if builder_decision.creates > 0:
        builder_create_intents = build_builder_vm_create_intents(
            config,
            builder_decision,
            vm_builder_submission_defaults_from_args(args, config),
            seed_prefix=args.seed_prefix,
        )
    create_intents = [*sandbox_create_intents, *builder_create_intents]
    requested_sandbox_stop_job_ids = stop_job_ids_from_decision(decision)
    requested_builder_stop_job_ids = stop_job_ids_from_decision(builder_decision)
    sandbox_stop_job_ids, blocked_sandbox_stop_job_ids = partition_safe_stop_job_ids(
        sandbox_nodes,
        requested_sandbox_stop_job_ids,
        deployment_id=config.deployment_id,
        allow_unlabeled=args.allow_unlabeled_stops,
        ownership_label=NODE_LABEL,
    )
    builder_stop_job_ids, blocked_builder_stop_job_ids = partition_safe_stop_job_ids(
        builder_nodes,
        requested_builder_stop_job_ids,
        deployment_id=config.deployment_id,
        allow_unlabeled=args.allow_unlabeled_stops,
        ownership_label=BUILDER_LABEL,
    )
    requested_stop_job_ids = (
        *requested_sandbox_stop_job_ids,
        *requested_builder_stop_job_ids,
    )
    stop_job_ids = (*sandbox_stop_job_ids, *builder_stop_job_ids)
    blocked_stop_job_ids = (
        *blocked_sandbox_stop_job_ids,
        *blocked_builder_stop_job_ids,
    )
    bootstrap_state_file = (
        getattr(args, "init_state_file", None) or config.bootstrap_file()
    )
    bootstrap_store = VmBootstrapStore(Path(bootstrap_state_file))
    bootstrap_records = prune_bootstrap_records(
        bootstrap_store.load(),
        {
            node.job_id
            for node in (*sandbox_nodes, *builder_nodes)
            if not node.job.is_final
        },
    )

    client: UCloudClient | None = None

    def get_client() -> UCloudClient:
        nonlocal client
        if client is None:
            client = UCloudClient(SessionStore(Path(config.ucloud_session_file)))
        return client

    def plan_bootstrap_from_payload(payload: dict[str, Any]) -> Any:
        plan = plan_vm_init(payload)
        if (
            getattr(args, "execute_init", False)
            and not getattr(args, "jobs_file", None)
            and not plan.runnable
            and "No SSH access command" in plan.reason
        ):
            job_id = str(payload.get("id") or "")
            if job_id:
                plan = plan_vm_init(
                    get_client().retrieve_job(
                        config.project_id, job_id, include_updates=True
                    )
                )
        return plan

    bootstrap_intents = build_vm_bootstrap_intents(
        [*sandbox_nodes, *builder_nodes],
        bootstrap_records,
        retry_seconds=max(0, int(getattr(args, "init_retry_seconds", 30))),
        max_per_cycle=max(0, int(getattr(args, "max_init_per_cycle", 1))),
        options_for_node=lambda node, role: vm_init_options_for_autoscaled_node(
            node,
            role,
            args,
            config,
        ),
        plan_for_payload=plan_bootstrap_from_payload,
    )
    bootstrap_intents = [
        apply_bootstrap_cli_requirements(intent)
        for intent in bootstrap_intents
        if intent.job_id not in stop_job_ids
    ]
    result: dict[str, Any] = {
        "projectId": config.project_id,
        "jobNamePrefix": config.job_name_prefix,
        "heartbeatFile": str(heartbeat_file),
        "bootstrapStateFile": str(bootstrap_state_file),
        "nodes": [node_to_dict(node) for node in nodes],
        "sandboxNodes": [node_to_dict(node) for node in sandbox_nodes],
        "builderNodes": [node_to_dict(node) for node in builder_nodes],
        "decision": scale_decision_to_dict(decision),
        "builderDecision": scale_decision_to_dict(builder_decision),
        "pendingImageBuilds": builder_pending,
        "preparedBuilderCount": builder_prepared,
        "createIntents": [intent.to_dict() for intent in create_intents],
        "sandboxCreateIntents": [intent.to_dict() for intent in sandbox_create_intents],
        "builderCreateIntents": [intent.to_dict() for intent in builder_create_intents],
        "createPayload": (
            bulk_payload_from_create_intents(create_intents)
            if create_intents
            else {"type": "bulk", "items": []}
        ),
        "requestedStopJobIds": list(requested_stop_job_ids),
        "stopJobIds": list(stop_job_ids),
        "blockedStopJobIds": list(blocked_stop_job_ids),
        "prunedFinalHeartbeats": list(final_heartbeat_job_ids),
        "removedStoppedHeartbeats": [],
        "bootstrapIntents": [
            vm_bootstrap_intent_to_dict(intent) for intent in bootstrap_intents
        ],
        "bootstrapResults": [],
        "executeCreates": args.execute,
        "executeStops": args.execute_stops,
        "executeInit": getattr(args, "execute_init", False),
        "allowUnlabeledStops": args.allow_unlabeled_stops,
        "rawNodes": nodes,
        "rawSandboxNodes": sandbox_nodes,
        "rawBuilderNodes": builder_nodes,
        "rawDecision": decision,
        "rawBuilderDecision": builder_decision,
        "rawCreateIntents": create_intents,
        "rawSandboxCreateIntents": sandbox_create_intents,
        "rawBuilderCreateIntents": builder_create_intents,
        "rawBootstrapIntents": bootstrap_intents,
    }

    if args.execute and create_intents:
        create_response = get_client().submit_jobs(
            config.project_id,
            bulk_payload_from_create_intents(create_intents),
        )
        result["createResponse"] = create_response
        result["createdJobIds"] = submitted_job_ids(create_response)

    if args.execute_stops and stop_job_ids:
        removed_stop_heartbeats = heartbeat_store.remove(stop_job_ids)
        result["removedStoppedHeartbeats"] = sorted(removed_stop_heartbeats)
        result["stopResponse"] = get_client().terminate_jobs(
            config.project_id, stop_job_ids
        )

    if getattr(args, "execute_init", False) and bootstrap_intents:
        bootstrap_results: list[dict[str, Any]] = []
        for intent in bootstrap_intents:
            if not intent.runnable or not intent.plan.ssh_command:
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
            attempt_started_at = utc_now()
            attempt_started_perf = time.perf_counter()
            stage_duration_ms: int | None = None
            run_duration_ms: int | None = None
            bootstrap_records = mark_bootstrap_attempt(bootstrap_records, intent)
            bootstrap_store.save(bootstrap_records)
            attempt_record = bootstrap_records.get(intent.job_id)
            attempt_count = (
                attempt_record.attempts
                if attempt_record is not None
                else intent.previous_attempts + 1
            )
            try:
                effective_options = intent.options
                stage_started_perf = time.perf_counter()
                stage_result = stage_vm_init_package_over_ssh(
                    intent.plan.ssh_command,
                    intent.options,
                    timeout_seconds=max(
                        1, int(getattr(args, "init_timeout_seconds", 1800))
                    ),
                    private_key_file=getattr(args, "init_ssh_private_key_file", None),
                )
                stage_elapsed_ms = int(
                    (time.perf_counter() - stage_started_perf) * 1000
                )
                stage_payload: dict[str, Any] | None = None
                if stage_result is not None:
                    stage_duration_ms = stage_elapsed_ms
                    stage_payload = {
                        "localPath": str(stage_result.local_path),
                        "remotePath": stage_result.remote_path,
                        "command": list(stage_result.command),
                        "returncode": stage_result.returncode,
                        "durationMs": stage_duration_ms,
                    }
                    if stage_result.returncode != 0:
                        error = (
                            "package staging exited with status "
                            f"{stage_result.returncode}"
                        )
                        bootstrap_records = mark_bootstrap_failure(
                            bootstrap_records,
                            intent,
                            error,
                        )
                        bootstrap_results.append(
                            {
                                "jobId": intent.job_id,
                                "nodeId": intent.node_id,
                                "role": intent.role,
                                "returncode": stage_result.returncode,
                                "status": "failed",
                                "error": error,
                                "packageStage": stage_payload,
                                "durationMs": _elapsed_ms(attempt_started_perf),
                            }
                        )
                        record_vm_init_attempt_result(
                            metrics_store,
                            intent,
                            status="failed",
                            attempts=attempt_count,
                            started_at=attempt_started_at,
                            attempt_started_perf=attempt_started_perf,
                            stage_duration_ms=stage_duration_ms,
                            run_duration_ms=run_duration_ms,
                            returncode=stage_result.returncode,
                            error=error,
                        )
                        continue
                    effective_options = replace(
                        intent.options,
                        package_spec=stage_result.remote_path,
                    )
                run_started_perf = time.perf_counter()
                run_result = run_init_over_ssh(
                    intent.plan.ssh_command,
                    render_vm_init_script(effective_options),
                    timeout_seconds=max(
                        1, int(getattr(args, "init_timeout_seconds", 1800))
                    ),
                    private_key_file=getattr(args, "init_ssh_private_key_file", None),
                )
                run_duration_ms = int((time.perf_counter() - run_started_perf) * 1000)
                if run_result.returncode == 0:
                    bootstrap_records = mark_bootstrap_success(
                        bootstrap_records, intent
                    )
                    bootstrap_results.append(
                        {
                            "jobId": intent.job_id,
                            "nodeId": intent.node_id,
                            "role": intent.role,
                            "returncode": 0,
                            "status": "succeeded",
                            "packageStage": stage_payload,
                            "durationMs": _elapsed_ms(attempt_started_perf),
                            "runDurationMs": run_duration_ms,
                        }
                    )
                    record_vm_init_attempt_result(
                        metrics_store,
                        intent,
                        status="succeeded",
                        attempts=attempt_count,
                        started_at=attempt_started_at,
                        attempt_started_perf=attempt_started_perf,
                        stage_duration_ms=stage_duration_ms,
                        run_duration_ms=run_duration_ms,
                        returncode=0,
                    )
                else:
                    error = f"init command exited with status {run_result.returncode}"
                    bootstrap_records = mark_bootstrap_failure(
                        bootstrap_records,
                        intent,
                        error,
                    )
                    bootstrap_results.append(
                        {
                            "jobId": intent.job_id,
                            "nodeId": intent.node_id,
                            "role": intent.role,
                            "returncode": run_result.returncode,
                            "status": "failed",
                            "error": error,
                            "packageStage": stage_payload,
                            "durationMs": _elapsed_ms(attempt_started_perf),
                            "runDurationMs": run_duration_ms,
                        }
                    )
                    record_vm_init_attempt_result(
                        metrics_store,
                        intent,
                        status="failed",
                        attempts=attempt_count,
                        started_at=attempt_started_at,
                        attempt_started_perf=attempt_started_perf,
                        stage_duration_ms=stage_duration_ms,
                        run_duration_ms=run_duration_ms,
                        returncode=run_result.returncode,
                        error=error,
                    )
            except Exception as exc:
                error = str(exc)
                bootstrap_records = mark_bootstrap_failure(
                    bootstrap_records,
                    intent,
                    error,
                )
                bootstrap_results.append(
                    {
                        "jobId": intent.job_id,
                        "nodeId": intent.node_id,
                        "role": intent.role,
                        "returncode": None,
                        "status": "failed",
                        "error": error,
                        "durationMs": _elapsed_ms(attempt_started_perf),
                    }
                )
                record_vm_init_attempt_result(
                    metrics_store,
                    intent,
                    status="failed",
                    attempts=attempt_count,
                    started_at=attempt_started_at,
                    attempt_started_perf=attempt_started_perf,
                    stage_duration_ms=stage_duration_ms,
                    run_duration_ms=run_duration_ms,
                    returncode=None,
                    error=error,
                )
            finally:
                bootstrap_store.save(bootstrap_records)
        result["bootstrapResults"] = bootstrap_results
    elif getattr(args, "execute_init", False):
        bootstrap_store.save(bootstrap_records)
    return result


def metrics_path_from_args(
    args: argparse.Namespace,
    config: AutoscalerConfig,
    *,
    sibling_file: Path | None = None,
) -> Path:
    explicit = getattr(args, "metrics_file", None)
    if explicit:
        return Path(explicit)
    if config.metrics_file:
        return config.metrics_path()
    if sibling_file is not None:
        return Path(sibling_file).expanduser().parent / "metrics.jsonl"
    return config.metrics_path()


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
) -> None:
    finished_at = utc_now()
    record_vm_init_attempt(
        metrics_store,
        job_id=intent.job_id,
        node_id=intent.node_id,
        role=intent.role,
        status=status,
        attempts=attempts,
        started_at=started_at.isoformat(),
        finished_at=finished_at.isoformat(),
        duration_ms=_elapsed_ms(attempt_started_perf),
        stage_duration_ms=stage_duration_ms,
        run_duration_ms=run_duration_ms,
        returncode=returncode,
        error=error,
    )


def _elapsed_ms(started_perf: float) -> int:
    return max(0, int((time.perf_counter() - started_perf) * 1000))


def load_jobs_for_plan(
    config: AutoscalerConfig, args: argparse.Namespace
) -> list[VmJob]:
    if args.jobs_file:
        payload = json.loads(args.jobs_file.read_text(encoding="utf-8"))
        raw_items = payload.get("items") if isinstance(payload, dict) else payload
    else:
        client = UCloudClient(SessionStore(Path(config.ucloud_session_file)))
        raw_items = client.browse_jobs(config.project_id, include_application=False)

    if not isinstance(raw_items, list):
        raise ValueError("Jobs payload must be a list or an object with an items list.")

    include_ids = {str(job_id) for job_id in args.include_job}
    jobs: list[VmJob] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        job = vm_job_from_payload(item)
        if should_include_job(job, config, include_ids, args.all_vm_jobs):
            jobs.append(job)
    return jobs


def policy_with_cli_overrides(
    policy: ScalePolicy,
    args: argparse.Namespace,
) -> ScalePolicy:
    effective = policy
    scale_down_seconds = getattr(args, "scale_down_idle_seconds", None)
    if scale_down_seconds is not None:
        effective = replace(
            effective,
            scale_down_idle_seconds=max(0, int(scale_down_seconds)),
        )
    builder_idle_seconds = getattr(args, "builder_scale_down_idle_seconds", None)
    if builder_idle_seconds is None:
        return effective
    return replace(
        effective,
        builder_scale_down_idle_seconds=max(0, int(builder_idle_seconds)),
    )


def should_include_job(
    job: VmJob,
    config: AutoscalerConfig,
    include_ids: set[str],
    all_vm_jobs: bool,
) -> bool:
    if job.id in include_ids:
        return True
    if (
        config.deployment_id
        and job.labels.get(DEPLOYMENT_LABEL) != config.deployment_id
    ):
        return False
    if not job_matches_private_network(job, config):
        return False
    if all_vm_jobs and job.is_vm:
        return True
    if job.labels.get(NODE_LABEL) == "true" or job.labels.get(BUILDER_LABEL) == "true":
        return True
    if job.name.startswith("ucloud-sandbox-builder"):
        return True
    return bool(config.job_name_prefix and job.name.startswith(config.job_name_prefix))


def sandbox_pool_nodes(nodes: list[Any], config: AutoscalerConfig) -> list[Any]:
    return [
        node
        for node in nodes
        if node.job.labels.get(NODE_LABEL) == "true"
        or (
            config.job_name_prefix
            and node.job.name.startswith(config.job_name_prefix)
            and node.job.labels.get(BUILDER_LABEL) != "true"
            and node.job.labels.get(GATEWAY_LABEL) != "true"
        )
    ]


def builder_pool_nodes(nodes: list[Any]) -> list[Any]:
    return [
        node
        for node in nodes
        if node.job.labels.get(BUILDER_LABEL) == "true"
        or node.job.name.startswith("ucloud-sandbox-builder")
    ]


def vm_init_options_for_autoscaled_node(
    node: SandboxNode,
    role: str,
    args: argparse.Namespace,
    config: AutoscalerConfig,
) -> VmInitOptions:
    node_agent_port = int(getattr(args, "init_node_agent_port", 8090))
    node_id = node.job.hostname or (node.heartbeat.node_id if node.heartbeat else "")
    if not node_id:
        node_id = f"ucloud-vm-{node.job.id}"
    labels = dict(node.job.labels)
    if role == "builder":
        labels.pop(NODE_LABEL, None)
        labels.setdefault(BUILDER_LABEL, "true")
    else:
        labels.setdefault(NODE_LABEL, "true")
    if config.deployment_id:
        labels.setdefault(DEPLOYMENT_LABEL, config.deployment_id)
    token_file = str(getattr(args, "init_heartbeat_bearer_token_file", "") or "")
    docker_quota_image_gb = max(
        0, int(getattr(args, "init_docker_quota_image_gb", 200))
    )
    total_resources = resources_from_vm_job(
        node.job, config.policy.default_node_resources
    )
    if docker_quota_image_gb > 0 and total_resources.disk_mb > 0:
        total_resources = replace(
            total_resources,
            disk_mb=min(total_resources.disk_mb, docker_quota_image_gb * 1024),
        )
    cpu_overcommit = max(0.0, float(getattr(args, "init_cpu_overcommit", 1.0)))
    memory_overcommit = max(0.0, float(getattr(args, "init_memory_overcommit", 1.0)))
    disk_overcommit = max(0.0, float(getattr(args, "init_disk_overcommit", 1.0)))
    if role == "builder":
        cpu_overcommit = 1.0
        memory_overcommit = 1.0
        disk_overcommit = 1.0
    return VmInitOptions(
        job_id=node.job.id,
        heartbeat_url=str(getattr(args, "init_heartbeat_url", "") or ""),
        heartbeat_bearer_token_file=token_file,
        heartbeat_bearer_token=read_bearer_token_source(
            token_file=token_file,
            source_file=getattr(args, "init_heartbeat_bearer_token_source_file", None),
        ),
        service_user=str(getattr(args, "init_service_user", "ucloud")),
        init_authorized_keys=read_prefixed_init_authorized_keys(args),
        node_id=node_id,
        work_dir=str(getattr(args, "init_work_dir", "/work/ucloud-sandboxes")),
        package_spec=str(getattr(args, "init_package_spec", "ucloud-sandboxes")),
        node_agent_host=str(getattr(args, "init_node_agent_host", "0.0.0.0")),
        node_agent_port=node_agent_port,
        node_url=f"http://{node_id}:{node_agent_port}",
        agent_version=node.job.labels.get(AGENT_VERSION_LABEL, package_version()),
        deployment_id=config.deployment_id or node.job.labels.get(DEPLOYMENT_LABEL, ""),
        init_version=node.job.labels.get(INIT_VERSION_LABEL, DEFAULT_INIT_VERSION),
        ssh_port_start=int(getattr(args, "init_ssh_port_start", 22000)),
        ssh_port_end=int(getattr(args, "init_ssh_port_end", 22999)),
        total_resources=total_resources,
        cpu_overcommit=cpu_overcommit,
        memory_overcommit=memory_overcommit,
        disk_overcommit=disk_overcommit,
        docker_quota_image_gb=docker_quota_image_gb,
        docker_insecure_registries=tuple(
            getattr(args, "init_docker_insecure_registry", []) or []
        ),
        host_aliases=tuple(getattr(args, "init_host_alias", []) or []),
        enable_image_builds=role == "builder",
        runtime_dry_run=bool(getattr(args, "init_runtime_dry_run", False)),
        heartbeat_interval_seconds=max(
            1,
            int(getattr(args, "init_heartbeat_interval_seconds", 20)),
        ),
        labels=labels,
    )


def apply_bootstrap_cli_requirements(intent: VmBootstrapIntent) -> VmBootstrapIntent:
    if not intent.runnable:
        return intent
    if not intent.options.heartbeat_url:
        return replace(
            intent,
            runnable=False,
            reason="init heartbeat url is required via --init-heartbeat-url",
        )
    if (
        intent.options.heartbeat_bearer_token_file
        and not intent.options.heartbeat_bearer_token
    ):
        return replace(
            intent,
            runnable=False,
            reason=(
                "heartbeat bearer token source is required via "
                "--init-heartbeat-bearer-token-source-file"
            ),
        )
    return intent


def vm_bootstrap_intent_to_dict(intent: VmBootstrapIntent) -> dict[str, Any]:
    return {
        "jobId": intent.job_id,
        "nodeId": intent.node_id,
        "role": intent.role,
        "runnable": intent.runnable,
        "reason": intent.reason,
        "sshCommand": intent.plan.ssh_command,
        "previousAttempts": intent.previous_attempts,
        "options": vm_init_options_to_dict(intent.options),
    }


def resources_from_vm_job(job: VmJob, default: ResourceQuantity) -> ResourceQuantity:
    return ResourceQuantity(
        vcpu=float(job.cpu) if job.cpu is not None else default.vcpu,
        memory_mb=(job.memory_gb * 1024)
        if job.memory_gb is not None
        else default.memory_mb,
        disk_mb=(job.disk_gb * 1024) if job.disk_gb is not None else default.disk_mb,
    )


def job_matches_private_network(job: VmJob, config: AutoscalerConfig) -> bool:
    if not config.private_network_id:
        return True
    return config.private_network_id in job.private_network_ids


def print_vm_job(job: VmJob) -> None:
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
    config: AutoscalerConfig,
    nodes: list[Any],
    decision: Any,
    heartbeat_file: Path,
    *,
    footer: str | None = "Dry-run only. Mutation commands are not implemented yet.",
) -> None:
    print(f"Project: {config.project_id}")
    print(f"Heartbeat file: {heartbeat_file}")
    print(
        "Nodes: "
        f"{decision.ready_nodes} ready, "
        f"{decision.provisioning_nodes} provisioning, "
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
                f" effective={resource_summary(node.heartbeat.effective_resources.to_dict())}"
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
    config: AutoscalerConfig,
    nodes: list[Any],
    decision: Any,
    heartbeat_file: Path,
    create_intents: list[VmCreateIntent],
    stop_job_ids: tuple[str, ...],
    result: dict[str, Any],
) -> None:
    print_plan(config, nodes, decision, heartbeat_file, footer=None)
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
        labels = intent.options.labels or {}
        role = "builder" if labels.get(BUILDER_LABEL) == "true" else "sandbox"
        print(
            f"- {intent.options.name} ({role}): host={intent.options.hostname} "
            f"url={intent.node_url}"
        )
    print("Stop intents:")
    requested_stop_job_ids = tuple(result.get("requestedStopJobIds", []))
    blocked_stop_job_ids = tuple(result.get("blockedStopJobIds", []))
    if not requested_stop_job_ids:
        print("- none")
    for job_id in stop_job_ids:
        print(f"- {job_id}")
    for job_id in blocked_stop_job_ids:
        print(f"- {job_id} (blocked: missing matching deployment label)")
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
    if result.get("createResponse") is not None:
        created = result.get("createdJobIds", [])
        created_label = ", ".join(created) if created else "(none returned)"
        print(f"Submitted create jobs: {created_label}")
    elif create_intents:
        print("Create dry-run only. Re-run with --execute to submit planned VMs.")
    if result.get("stopResponse") is not None:
        print(f"Executed stop requests: {', '.join(stop_job_ids)}")
        if blocked_stop_job_ids:
            print(f"Skipped blocked stop requests: {', '.join(blocked_stop_job_ids)}")
    elif requested_stop_job_ids:
        if blocked_stop_job_ids:
            if result.get("executeStops"):
                print(
                    "No stop requests executed. Blocked jobs require matching "
                    "--deployment-id or --allow-unlabeled-stops."
                )
            else:
                print(
                    "Stop dry-run only. Blocked jobs require matching --deployment-id "
                    "or --allow-unlabeled-stops."
                )
        else:
            print(
                "Stop dry-run only. Re-run with --execute-stops to terminate planned jobs."
            )


def vm_job_to_dict(job: VmJob) -> dict[str, Any]:
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
    }
    if node.heartbeat is not None:
        raw["heartbeat"] = heartbeat_to_dict(node.heartbeat)
    return raw


def scale_decision_to_dict(decision: Any) -> dict[str, Any]:
    return {
        "actions": [asdict(action) for action in decision.actions],
        "readyNodes": decision.ready_nodes,
        "provisioningNodes": decision.provisioning_nodes,
        "totalNodes": decision.total_nodes,
        "pendingResources": decision.pending_resources.to_dict(),
        "preparedResources": decision.prepared_resources.to_dict(),
        "desiredResources": decision.desired_resources.to_dict(),
        "projectedFreeResources": decision.projected_free_resources.to_dict(),
        "resourceDeficit": decision.resource_deficit.to_dict(),
        "reasons": list(decision.reasons),
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


def sandbox_demand_from_args(args: argparse.Namespace) -> SandboxDemand:
    return SandboxDemand(
        pending_resources=ResourceQuantity(
            vcpu=max(0.0, args.pending_vcpu),
            memory_mb=max(0, args.pending_memory_mb),
            disk_mb=max(0, args.pending_disk_mb),
        ),
        oldest_pending_seconds=max(0, args.oldest_pending_seconds),
    )


def vm_node_submission_defaults_from_args(
    args: argparse.Namespace,
    config: AutoscalerConfig,
) -> VmNodeSubmissionDefaults:
    if args.no_private_network:
        private_network_id = None
    else:
        private_network_id = args.private_network_id or config.private_network_id
        if not private_network_id:
            raise ValueError(
                "private network id is required via --private-network-id or config; "
                "use --no-private-network to submit without one."
            )

    ssh_requested = bool(getattr(args, "ssh", False))
    ssh_disabled = bool(getattr(args, "no_ssh", False))
    if ssh_requested and ssh_disabled:
        raise ValueError("--ssh and --no-ssh cannot be used together.")

    labels = parse_labels(args.label)
    labels.setdefault(NODE_LABEL, "true")
    if config.deployment_id:
        labels.setdefault(DEPLOYMENT_LABEL, config.deployment_id)
    labels.setdefault(AGENT_VERSION_LABEL, package_version())
    labels.setdefault(INIT_VERSION_LABEL, DEFAULT_INIT_VERSION)

    return VmNodeSubmissionDefaults(
        private_network_id=private_network_id,
        product=VmProductRef(
            id=args.product_id,
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
    )


def vm_builder_submission_defaults_from_args(
    args: argparse.Namespace,
    config: AutoscalerConfig,
) -> VmNodeSubmissionDefaults:
    if args.no_private_network:
        private_network_id = None
    else:
        private_network_id = args.private_network_id or config.private_network_id
        if not private_network_id:
            raise ValueError(
                "private network id is required via --private-network-id or config; "
                "use --no-private-network to submit without one."
            )

    ssh_requested = bool(getattr(args, "ssh", False))
    ssh_disabled = bool(getattr(args, "no_ssh", False))
    if ssh_requested and ssh_disabled:
        raise ValueError("--ssh and --no-ssh cannot be used together.")

    labels = parse_labels(args.label)
    labels.pop(NODE_LABEL, None)
    labels.setdefault(BUILDER_LABEL, "true")
    if config.deployment_id:
        labels.setdefault(DEPLOYMENT_LABEL, config.deployment_id)
    labels.setdefault(AGENT_VERSION_LABEL, package_version())
    labels.setdefault(INIT_VERSION_LABEL, DEFAULT_INIT_VERSION)

    return VmNodeSubmissionDefaults(
        private_network_id=private_network_id,
        product=VmProductRef(
            id=args.builder_product_id,
            category=args.product_category,
            provider=args.product_provider,
        ),
        application=VmApplicationRef(
            name=args.app_name,
            version=args.app_version,
        ),
        disk_gb=args.builder_disk_gb,
        time_allocation=VmTimeAllocation(
            hours=args.time_hours,
            minutes=args.time_minutes,
            seconds=args.time_seconds,
        ),
        ssh_enabled=ssh_requested,
        allow_duplicate_job=args.allow_duplicate_job,
        labels=labels,
    )


def vm_submission_options_from_args(
    args: argparse.Namespace,
    config: AutoscalerConfig,
) -> tuple[VmSubmissionOptions, str]:
    role = getattr(args, "role", "node")
    private_network_id: str | None
    if args.no_private_network:
        private_network_id = None
    else:
        private_network_id = args.private_network_id or config.private_network_id
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
            config.gateway_public_link_id if role == "gateway" else None
        )
    public_link_port = (
        getattr(args, "public_link_port", None)
        if getattr(args, "public_link_port", None) is not None
        else config.gateway_public_link_port or DEFAULT_PUBLIC_LINK_PORT
    )

    seed = args.hostname_seed or uuid4().hex[:8]
    hostname_prefix = args.hostname_prefix or (
        "sandbox-gateway"
        if role == "gateway"
        else "sandbox-builder"
        if role == "builder"
        else config.node_hostname_prefix
    )
    hostname = args.hostname or stable_hostname(seed, prefix=hostname_prefix)
    if role == "gateway":
        default_name_prefix = "ucloud-sandbox-gateway"
    elif role == "builder":
        default_name_prefix = "ucloud-sandbox-builder"
    else:
        default_name_prefix = config.job_name_prefix.rstrip("-")
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

    return (
        VmSubmissionOptions(
            name=name,
            hostname=hostname,
            private_network_id=private_network_id,
            public_link_id=public_link_id,
            public_link_port=public_link_port,
            product=VmProductRef(
                id=args.product_id,
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


def read_init_authorized_keys(args: argparse.Namespace) -> tuple[str, ...]:
    keys: list[str] = []
    keys.extend(getattr(args, "init_authorized_key", []) or [])
    for path in getattr(args, "init_authorized_key_file", []) or []:
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            key = line.strip()
            if key and not key.startswith("#"):
                keys.append(key)
    return tuple(keys)


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


def read_prefixed_init_authorized_keys(args: argparse.Namespace) -> tuple[str, ...]:
    keys: list[str] = []
    keys.extend(getattr(args, "init_authorized_key", []) or [])
    for path in getattr(args, "init_authorized_key_file", []) or []:
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            key = line.strip()
            if key and not key.startswith("#"):
                keys.append(key)
    return tuple(keys)


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


def vm_init_options_from_args(args: argparse.Namespace, job_id: str) -> VmInitOptions:
    cpu_overcommit = args.cpu_overcommit
    memory_overcommit = args.memory_overcommit
    disk_overcommit = args.disk_overcommit
    if args.enable_image_builds:
        cpu_overcommit = 1.0
        memory_overcommit = 1.0
        disk_overcommit = 1.0
    return VmInitOptions(
        job_id=job_id,
        heartbeat_url=args.heartbeat_url,
        heartbeat_bearer_token_file=args.heartbeat_bearer_token_file,
        heartbeat_bearer_token=read_bearer_token_source(
            token_file=args.heartbeat_bearer_token_file,
            source_file=getattr(args, "heartbeat_bearer_token_source_file", None),
        ),
        service_user=args.service_user,
        init_authorized_keys=read_init_authorized_keys(args),
        node_id=args.node_id or "",
        work_dir=args.work_dir,
        package_spec=args.package_spec,
        node_agent_host=args.node_agent_host,
        node_agent_port=args.node_agent_port,
        node_url=args.node_url or "",
        agent_version=args.agent_version,
        deployment_id=getattr(args, "deployment_id", "") or "",
        init_version=args.init_version,
        ssh_port_start=args.ssh_port_start,
        ssh_port_end=args.ssh_port_end,
        total_resources=resource_quantity_from_args(args),
        cpu_overcommit=cpu_overcommit,
        memory_overcommit=memory_overcommit,
        disk_overcommit=disk_overcommit,
        docker_quota_image_gb=args.docker_quota_image_gb,
        docker_insecure_registries=tuple(args.docker_insecure_registry or []),
        host_aliases=tuple(args.host_alias or []),
        enable_image_builds=args.enable_image_builds,
        runtime_dry_run=args.runtime_dry_run,
        heartbeat_interval_seconds=args.heartbeat_interval_seconds,
        labels=parse_labels(args.label),
    )


def vm_init_options_to_dict(options: VmInitOptions) -> dict[str, Any]:
    return {
        "jobId": options.job_id,
        "nodeId": options.normalized_node_id(),
        "heartbeatUrl": options.heartbeat_url,
        "heartbeatBearerTokenFile": options.heartbeat_bearer_token_file,
        "serviceUser": options.service_user,
        "initAuthorizedKeys": list(options.init_authorized_keys),
        "workDir": options.work_dir,
        "packageSpec": options.package_spec,
        "nodeAgentHost": options.node_agent_host,
        "nodeAgentPort": options.node_agent_port,
        "nodeUrl": options.advertised_node_url(),
        "agentVersion": options.agent_version,
        "deploymentId": options.deployment_id,
        "initVersion": options.init_version,
        "sshPortStart": options.ssh_port_start,
        "sshPortEnd": options.ssh_port_end,
        "totalResources": options.total_resources.to_dict(),
        "cpuOvercommit": options.cpu_overcommit,
        "memoryOvercommit": options.memory_overcommit,
        "diskOvercommit": options.disk_overcommit,
        "dockerQuotaImageGb": options.docker_quota_image_gb,
        "dockerInsecureRegistries": list(options.docker_insecure_registries),
        "hostAliases": list(options.host_aliases),
        "enableImageBuilds": options.enable_image_builds,
        "runtimeDryRun": options.runtime_dry_run,
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
