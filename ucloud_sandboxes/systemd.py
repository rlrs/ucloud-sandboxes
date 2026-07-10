from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
from typing import Callable, Sequence

from .managed_registry import registry_maintenance_lock


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def run_registry_gc(
    *,
    data_dir: Path,
    registry_image: str,
    lock_file: Path,
    runner: CommandRunner = subprocess.run,
) -> None:
    """Run offline Distribution GC while holding the shared maintenance fence."""

    with registry_maintenance_lock(lock_file, blocking=False):
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
    registry_gc.add_argument("--data-dir", type=Path, required=True)
    registry_gc.add_argument("--registry-image", required=True)
    registry_gc.add_argument("--lock-file", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "registry-gc":
        run_registry_gc(
            data_dir=args.data_dir,
            registry_image=args.registry_image,
            lock_file=args.lock_file,
        )
        return 0
    raise ValueError(f"unsupported systemd helper: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
