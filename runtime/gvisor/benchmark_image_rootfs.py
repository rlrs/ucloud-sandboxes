#!/usr/bin/env python3
"""Benchmark Docker-as-image-infrastructure plus direct overlay materialization."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import time

from ucloud_sandboxes.image_rootfs import (
    DockerRootfsStore,
    OverlayRootfsManager,
)


def directory_allocated_bytes(root: Path) -> int:
    return sum(
        path.lstat().st_blocks * 512
        for path in root.rglob("*")
        if not path.is_symlink()
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--config-template", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("benchmark must run as root")
    template = json.loads(args.config_template.read_text(encoding="utf-8"))
    store = DockerRootfsStore((args.root / "cache").resolve())

    started = time.monotonic()
    image = store.materialize(args.image)
    cold_materialize_ms = (time.monotonic() - started) * 1000
    started = time.monotonic()
    cached = store.materialize(args.image)
    cached_materialize_ms = (time.monotonic() - started) * 1000
    if image != cached:
        raise RuntimeError("cached materialization changed identity")

    manager = OverlayRootfsManager(
        store,
        writable_root=(args.root / "writable").resolve(),
        bundle_root=(args.root / "bundles").resolve(),
    )
    started = time.monotonic()
    lease = manager.prepare(
        sandbox_id="image-benchmark",
        sandbox_generation=1,
        image_ref=args.image,
        config_template=template,
    )
    overlay_prepare_ms = (time.monotonic() - started) * 1000
    try:
        mountpoint = (
            subprocess.run(
                ("mountpoint", "--quiet", str(lease.merged)),
                check=False,
            ).returncode
            == 0
        )
        root_entries = sorted(path.name for path in lease.merged.iterdir())
    finally:
        started = time.monotonic()
        manager.release(lease)
        overlay_release_ms = (time.monotonic() - started) * 1000

    exporter_containers = subprocess.run(
        (
            "docker",
            "ps",
            "--all",
            "--quiet",
            "--filter",
            "label=dev.ucloud-sandboxes.image-export=true",
        ),
        text=True,
        stdout=subprocess.PIPE,
        check=True,
    ).stdout.split()
    payload = {
        "schema": 1,
        "image": args.image,
        "image_id": image.image_id,
        "rootfs_identity_sha256": image.rootfs_identity_sha256,
        "cold_materialize_ms": cold_materialize_ms,
        "cached_materialize_ms": cached_materialize_ms,
        "base_allocated_bytes": directory_allocated_bytes(image.rootfs),
        "overlay_prepare_ms": overlay_prepare_ms,
        "overlay_release_ms": overlay_release_ms,
        "overlay_was_mountpoint": mountpoint,
        "root_entries": root_entries,
        "exporter_container_leaks": exporter_containers,
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
