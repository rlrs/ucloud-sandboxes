from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
from typing import Callable, Sequence

from .config import DeploymentConfig
from .managed_registry import registry_maintenance_lock


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def require_registry_mount(config: DeploymentConfig) -> None:
    mount_point = Path(config.registry_mount_point)
    if not mount_point.is_mount():
        raise RuntimeError(f"registry storage is not mounted at {mount_point}")


def run_registry_gc(
    *,
    data_dir: Path,
    registry_image: str,
    lock_file: Path,
    runner: CommandRunner = subprocess.run,
) -> None:
    """Run offline Distribution GC while holding the shared maintenance fence."""

    with registry_maintenance_lock(lock_file, blocking=False):
        # Distribution exits non-zero when its repository tree has never been
        # created. That is the normal state of a fresh deployment, not a GC
        # failure. Check under the same maintenance fence used by publishers
        # so the empty-registry decision cannot race a managed push.
        repositories_dir = data_dir / "docker" / "registry" / "v2" / "repositories"
        if not repositories_dir.exists():
            return
        runner(
            ["systemctl", "stop", "ucloud-sandbox-registry.service"],
            check=True,
            text=True,
        )
        try:
            runner(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{data_dir}:/var/lib/registry",
                    registry_image,
                    "garbage-collect",
                    "--delete-untagged",
                    "/etc/docker/registry/config.yml",
                ],
                check=True,
                text=True,
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


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = DeploymentConfig.from_file(args.config)
    if args.command == "registry-gc":
        require_registry_mount(config)
        run_registry_gc(
            data_dir=config.registry_data_dir(),
            registry_image="registry:2",
            lock_file=Path("/run/lock/ucloud-sandbox-registry-maintenance"),
        )
        return 0
    if args.command == "registry":
        require_registry_mount(config)
        data_dir = config.registry_data_dir()
        data_dir.mkdir(parents=True, exist_ok=True)
        command = [
            "docker",
            "run",
            "--rm",
            "--name",
            "ucloud-sandbox-registry",
            "-p",
            f"0.0.0.0:{config.registry_port}:5000",
            "-v",
            f"{data_dir}:/var/lib/registry",
            "-e",
            "REGISTRY_STORAGE_DELETE_ENABLED=true",
            "registry:2",
        ]
        os.execvp(command[0], command)
    raise ValueError(f"unsupported systemd helper: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
