#!/usr/bin/env python3
"""Derive a builder bundle from a qualified sandbox bundle.

The two roles share the verified OS package and kernel closure. Builders add
Docker Buildx and deliberately drop sandbox-only runsc, managed-init, and
storage-native artifacts. The output remains deterministic and receives a
fresh role-specific manifest and checksum.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

from scripts.repack_node_bundle import (
    build_bundle,
    extract_tar,
    sha256_file,
    validate_source_bundle,
)
from ucloud_sandboxes.vm_init import (
    BUILDER_RUNTIME_PACKAGES,
    SANDBOX_RUNTIME_PACKAGES,
)


def deb_fields(path: Path) -> tuple[str, str]:
    result = subprocess.run(
        ["dpkg-deb", "--field", str(path), "Package", "Architecture"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[:512]
        raise ValueError(f"cannot inspect Buildx package: {detail}")
    fields: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition(":")
        if separator:
            fields[key.strip()] = value.strip()
    package = fields.get("Package", "")
    architecture = fields.get("Architecture", "")
    if package != "docker-buildx-plugin" or not architecture:
        raise ValueError("Buildx input is not a docker-buildx-plugin package")
    return package, architecture


def derive_builder_bundle(
    source: Path,
    buildx_deb: Path,
    output: Path,
) -> None:
    if output.resolve() == source.resolve():
        raise ValueError("output must not replace the qualified source bundle")
    _, buildx_architecture = deb_fields(buildx_deb)

    with tempfile.TemporaryDirectory(prefix="ucloud-builder-bundle-") as raw_work:
        root = Path(raw_work) / "bundle"
        root.mkdir()
        extract_tar(source, root)
        manifest_path = root / "package-bundle.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        validate_source_bundle(root, manifest)
        runtime = manifest.get("runtime")
        if not isinstance(runtime, dict):
            raise TypeError("source bundle has no runtime manifest")
        if runtime.get("role") != "sandbox":
            raise ValueError("source bundle must have role=sandbox")
        if runtime.get("packages") != list(SANDBOX_RUNTIME_PACKAGES):
            raise ValueError("source bundle has an unexpected sandbox package set")
        platform = runtime.get("platform")
        if (
            not isinstance(platform, dict)
            or platform.get("architecture") != buildx_architecture
        ):
            raise ValueError("Buildx package architecture does not match the bundle")

        deb_dir = root / "runtime/debs"
        destination = deb_dir / buildx_deb.name
        if destination.exists():
            raise ValueError("source bundle already contains this Buildx package")
        shutil.copyfile(buildx_deb, destination)
        runtime["role"] = "builder"
        runtime["packages"] = list(BUILDER_RUNTIME_PACKAGES)
        runtime["files"] = [
            {
                "name": path.name,
                "sha256": sha256_file(path),
                "size": path.stat().st_size,
            }
            for path in sorted(deb_dir.glob("*.deb"), key=lambda item: item.name)
        ]
        for section in ("direct_runsc", "managed_init", "storage_native"):
            runtime.pop(section, None)
        for relative in ("runtime/direct", "runtime/storage-native"):
            target = root / relative
            if target.exists():
                shutil.rmtree(target)

        manifest_bytes = (
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        )
        build_bundle(root, manifest_bytes, output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--buildx-deb", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    for path in (args.source, args.buildx_deb):
        if not path.is_file():
            raise SystemExit(f"required input does not exist: {path}")
    derive_builder_bundle(args.source, args.buildx_deb, args.output)
    print(f"bundle={args.output}")
    print(f"sha256={sha256_file(args.output)}")


if __name__ == "__main__":
    main()
