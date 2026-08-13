from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
from typing import Callable, Mapping, Sequence

from .config import DeploymentConfig
from .managed_registry import registry_maintenance_lock


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
REGISTRY_IMAGE = "registry:3.1.1"
REGISTRY_CONFIG_PATH = "/etc/distribution/config.yml"
REGISTRY_S3_CHUNK_BYTES = 32 * 1024 * 1024


def require_registry_mount(config: DeploymentConfig) -> None:
    if config.registry_store.kind != "filesystem":
        return
    mount_point = Path(config.registry_mount_point)
    if not mount_point.is_mount():
        raise RuntimeError(f"registry storage is not mounted at {mount_point}")


def run_registry_gc(
    *,
    config: DeploymentConfig,
    lock_file: Path,
    runner: CommandRunner = subprocess.run,
    environ: Mapping[str, str] | None = None,
) -> None:
    """Run offline Distribution GC while holding the shared maintenance fence."""

    with registry_maintenance_lock(lock_file, blocking=False):
        # Distribution exits non-zero when its repository tree has never been
        # created. That is the normal state of a fresh deployment, not a GC
        # failure. Check under the same maintenance fence used by publishers
        # so the empty-registry decision cannot race a managed push.
        if config.registry_store.kind == "filesystem":
            repositories_dir = (
                config.registry_data_dir()
                / "docker"
                / "registry"
                / "v2"
                / "repositories"
            )
            if not repositories_dir.exists():
                return
        environment = registry_process_environment(config, environ=environ)
        runner(
            ["systemctl", "stop", "ucloud-sandbox-registry.service"],
            check=True,
            text=True,
        )
        try:
            runner(
                registry_gc_command(config),
                check=True,
                text=True,
                env=environment,
            )
        finally:
            runner(
                ["systemctl", "start", "ucloud-sandbox-registry.service"],
                check=True,
                text=True,
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="UCloud systemd service helpers")
    subparsers = parser.add_subparsers(dest="command", required=True)
    registry_gc = subparsers.add_parser(
        "registry-gc",
        help="run fenced offline Docker Distribution garbage collection",
    )
    registry_gc.add_argument("--config", type=Path, required=True)
    registry = subparsers.add_parser(
        "registry",
        help="run the deployment Docker Distribution service",
    )
    registry.add_argument("--config", type=Path, required=True)
    return parser


def registry_run_command(config: DeploymentConfig) -> list[str]:
    """Build the registry command without Docker's kernel port-publish path."""

    return [
        "docker",
        "run",
        "--rm",
        "--name",
        "ucloud-sandbox-registry",
        "--network",
        "host",
        *_registry_storage_docker_args(config),
        *_registry_forwarded_environment_args(config),
        REGISTRY_IMAGE,
    ]


def registry_gc_command(config: DeploymentConfig) -> list[str]:
    return [
        "docker",
        "run",
        "--rm",
        *_registry_storage_docker_args(config),
        *_registry_forwarded_environment_args(config),
        REGISTRY_IMAGE,
        "garbage-collect",
        "--delete-untagged",
        REGISTRY_CONFIG_PATH,
    ]


def registry_process_environment(
    config: DeploymentConfig,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Resolve registry settings without exposing S3 secrets in process args."""

    result = dict(os.environ if environ is None else environ)
    result["REGISTRY_STORAGE_DELETE_ENABLED"] = "true"
    result["REGISTRY_HTTP_ADDR"] = f"0.0.0.0:{config.registry_port}"
    result["REGISTRY_HTTP_DEBUG_ADDR"] = "127.0.0.1:5001"
    result["REGISTRY_LOG_LEVEL"] = "info"
    result["OTEL_TRACES_EXPORTER"] = "none"
    store = config.registry_store
    result["REGISTRY_STORAGE"] = store.kind
    if store.kind == "filesystem":
        result["REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY"] = "/var/lib/registry"
        return result
    access_key = result.get(store.access_key_id_env, "").strip()
    secret_key = result.get(store.secret_access_key_env, "").strip()
    if not access_key or not secret_key:
        raise RuntimeError(
            "S3 registry credentials are missing from "
            f"{store.access_key_id_env} and {store.secret_access_key_env}"
        )
    result.update(
        {
            "REGISTRY_STORAGE_S3_ACCESSKEY": access_key,
            "REGISTRY_STORAGE_S3_SECRETKEY": secret_key,
            "REGISTRY_STORAGE_S3_REGION": store.region,
            "REGISTRY_STORAGE_S3_REGIONENDPOINT": store.endpoint,
            "REGISTRY_STORAGE_S3_FORCEPATHSTYLE": str(
                store.force_path_style
            ).lower(),
            "REGISTRY_STORAGE_S3_BUCKET": store.bucket,
            "REGISTRY_STORAGE_S3_ROOTDIRECTORY": store.prefix,
            "REGISTRY_STORAGE_S3_SECURE": str(
                store.endpoint.startswith("https://")
            ).lower(),
            "REGISTRY_STORAGE_S3_V4AUTH": "true",
            "REGISTRY_STORAGE_S3_CHUNKSIZE": str(REGISTRY_S3_CHUNK_BYTES),
        }
    )
    return result


def _registry_storage_docker_args(config: DeploymentConfig) -> list[str]:
    if config.registry_store.kind == "filesystem":
        return ["-v", f"{config.registry_data_dir()}:/var/lib/registry"]
    return []


def _registry_forwarded_environment_args(config: DeploymentConfig) -> list[str]:
    names = [
        "REGISTRY_STORAGE_DELETE_ENABLED",
        "REGISTRY_HTTP_ADDR",
        "REGISTRY_HTTP_DEBUG_ADDR",
        "REGISTRY_LOG_LEVEL",
        "OTEL_TRACES_EXPORTER",
        "REGISTRY_STORAGE",
    ]
    if config.registry_store.kind == "filesystem":
        names.append("REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY")
    else:
        names.extend(
            (
                "REGISTRY_STORAGE_S3_ACCESSKEY",
                "REGISTRY_STORAGE_S3_SECRETKEY",
                "REGISTRY_STORAGE_S3_REGION",
                "REGISTRY_STORAGE_S3_REGIONENDPOINT",
                "REGISTRY_STORAGE_S3_FORCEPATHSTYLE",
                "REGISTRY_STORAGE_S3_BUCKET",
                "REGISTRY_STORAGE_S3_ROOTDIRECTORY",
                "REGISTRY_STORAGE_S3_SECURE",
                "REGISTRY_STORAGE_S3_V4AUTH",
                "REGISTRY_STORAGE_S3_CHUNKSIZE",
            )
        )
    return [item for name in names for item in ("-e", name)]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = DeploymentConfig.from_file(args.config)
    if args.command == "registry-gc":
        require_registry_mount(config)
        run_registry_gc(
            config=config,
            lock_file=Path("/run/lock/ucloud-sandbox-registry-maintenance"),
        )
        return 0
    if args.command == "registry":
        require_registry_mount(config)
        if config.registry_store.kind == "filesystem":
            config.registry_data_dir().mkdir(parents=True, exist_ok=True)
        command = registry_run_command(config)
        environment = registry_process_environment(config)
        os.execvpe(command[0], command, environment)
    raise ValueError(f"unsupported systemd helper: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
