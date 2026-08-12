#!/usr/bin/env python3
"""Repack a qualified node bundle with a new agent wheel and storage backend.

This intentionally preserves the source bundle's OS packages, kernel-module
closure, runsc, and managed init.  It is useful when those expensive artifacts
have already been qualified for an unchanged OS image, but the pure-Python node
agent or the content-addressed storage backend has changed.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tarfile
import tempfile
import zipfile

from ucloud_sandboxes.vm_init import PINNED_STORAGE_NATIVE_AGENTENV_COMMIT


EXPECTED_STORAGE_PATCHES = [
    "agentenv-streaming-dense-export.patch",
    "agentenv-pooled-delete.patch",
    "agentenv-owner-identity.patch",
]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_relative_path(raw: str) -> Path:
    path = PurePosixPath(raw)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"unsafe archive path: {raw!r}")
    return Path(*path.parts)


def extract_tar(archive_path: Path, destination: Path) -> None:
    with tarfile.open(archive_path, "r:*") as archive:
        for member in archive.getmembers():
            relative = safe_relative_path(member.name)
            target = destination / relative
            if member.issym() or member.islnk():
                safe_relative_path(member.linkname)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                target.chmod(member.mode)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise ValueError(f"could not read {member.name!r}")
                with target.open("wb") as output:
                    shutil.copyfileobj(source, output)
                target.chmod(member.mode)
            else:
                raise ValueError(f"unsupported archive member: {member.name!r}")


def validate_digest(path: Path, expected: str, description: str) -> None:
    actual = sha256_file(path)
    if actual != expected:
        raise ValueError(
            f"{description} digest mismatch: expected {expected}, got {actual}"
        )


def validate_source_bundle(root: Path, manifest: dict[str, object]) -> None:
    runtime = manifest.get("runtime")
    if manifest.get("version") != 1 or not isinstance(runtime, dict):
        raise ValueError("unsupported package bundle manifest")

    files = runtime.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("source bundle has no runtime package files")
    for item in files:
        if not isinstance(item, dict):
            raise ValueError("invalid runtime package entry")
        path = root / "runtime/debs" / str(item["name"])
        validate_digest(path, str(item["sha256"]), f"runtime package {path.name}")

    for section, default_root in (
        ("agent", root),
        ("direct_runsc", root),
        ("managed_init", root),
        ("storage_native", root),
    ):
        item = runtime.get(section)
        if not isinstance(item, dict):
            raise ValueError(f"source bundle is missing {section}")
        path = default_root / str(item["file"])
        validate_digest(path, str(item["sha256"]), section)

    kernel = runtime.get("kernel")
    if not isinstance(kernel, dict) or not isinstance(kernel.get("files"), list):
        raise ValueError("source bundle has no kernel-module closure")
    release = str(kernel["release"])
    for item in kernel["files"]:
        if not isinstance(item, dict):
            raise ValueError("invalid kernel-module entry")
        path = root / "runtime/kernel" / release / str(item["name"])
        validate_digest(path, str(item["sha256"]), f"kernel module {path.name}")


def inspect_wheel(wheel: Path) -> tuple[str, str]:
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        wheel_metadata_names = [
            name for name in names if name.endswith(".dist-info/WHEEL")
        ]
        dist_info_names = {
            name.split("/", 1)[0]
            for name in names
            if name.endswith(".dist-info/METADATA")
        }
        if len(wheel_metadata_names) != 1 or len(dist_info_names) != 1:
            raise ValueError("wheel must contain exactly one distribution")
        metadata = archive.read(wheel_metadata_names[0]).decode("utf-8")
        if "Root-Is-Purelib: true" not in metadata or "Tag: py3-none-any" not in metadata:
            raise ValueError("only a py3-none-any wheel can update this Linux bundle")
        if not any(name.startswith("ucloud_sandboxes/") for name in names):
            raise ValueError("wheel does not contain the ucloud_sandboxes package")
        return "ucloud_sandboxes", next(iter(dist_info_names))


def replace_agent_package(runtime_root: Path, wheel: Path) -> None:
    package_name, dist_info_name = inspect_wheel(wheel)
    site_packages = runtime_root / "site-packages"
    if not site_packages.is_dir():
        raise ValueError("agent archive has no site-packages directory")

    package_path = site_packages / package_name
    if package_path.exists():
        shutil.rmtree(package_path)
    for old_dist_info in site_packages.glob("ucloud_sandboxes-*.dist-info"):
        shutil.rmtree(old_dist_info)

    with zipfile.ZipFile(wheel) as archive:
        wanted_prefixes = (f"{package_name}/", f"{dist_info_name}/")
        for member in archive.infolist():
            if not member.filename.startswith(wanted_prefixes):
                continue
            relative = safe_relative_path(member.filename)
            target = site_packages / relative
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output)
            mode = member.external_attr >> 16
            target.chmod(stat.S_IMODE(mode) if mode else 0o644)


def normalized_tar_info(info: tarfile.TarInfo) -> tarfile.TarInfo:
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    return info


def add_tree(archive: tarfile.TarFile, root: Path) -> None:
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        arcname = path.relative_to(root).as_posix()
        archive.add(path, arcname=arcname, recursive=False, filter=normalized_tar_info)


def build_agent_archive(runtime_root: Path, output: Path) -> None:
    with tarfile.open(output, "w") as archive:
        add_tree(archive, runtime_root)


def validate_storage_build(
    backend: Path, manifest_path: Path, license_path: Path
) -> dict[str, object]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("schema") != 3 or payload.get("license") != "MIT":
        raise ValueError("unsupported storage-native build manifest")
    if payload.get("agentenv_commit") != PINNED_STORAGE_NATIVE_AGENTENV_COMMIT:
        raise ValueError("storage-native AgentEnv commit is not pinned")
    patches = payload.get("patches")
    if not isinstance(patches, list) or [
        item.get("name") for item in patches if isinstance(item, dict)
    ] != EXPECTED_STORAGE_PATCHES:
        raise ValueError("storage-native patch set is not the expected set")
    if any(
        not isinstance(item, dict)
        or len(str(item.get("sha256") or "")) != 64
        for item in patches
    ):
        raise ValueError("storage-native patch provenance is incomplete")
    validate_digest(
        backend, str(payload.get("artifact_sha256")), "storage-native backend"
    )
    if not license_path.is_file():
        raise ValueError("storage-native license is absent")
    return payload


def update_manifest(
    manifest: dict[str, object],
    *,
    agent_archive: Path,
    storage_backend: Path,
    storage_manifest: Path,
    storage_license: Path,
    storage_build: dict[str, object],
    kernel_release: str | None,
    kernel_module_dir: Path | None,
) -> bytes:
    runtime = manifest["runtime"]
    if not isinstance(runtime, dict):
        raise ValueError("invalid package bundle runtime")
    agent = runtime["agent"]
    storage = runtime["storage_native"]
    if not isinstance(agent, dict) or not isinstance(storage, dict):
        raise ValueError("invalid package bundle artifacts")

    agent.update(
        sha256=sha256_file(agent_archive), size=agent_archive.stat().st_size
    )
    storage.update(
        agentenv_commit=storage_build["agentenv_commit"],
        host_architecture=storage_build.get("host_architecture"),
        sha256=sha256_file(storage_backend),
        size=storage_backend.stat().st_size,
        manifest_sha256=sha256_file(storage_manifest),
        license_sha256=sha256_file(storage_license),
    )
    if kernel_release is not None and kernel_module_dir is not None:
        kernel = runtime.get("kernel")
        if not isinstance(kernel, dict):
            raise ValueError("invalid package bundle kernel closure")
        module_files = sorted(kernel_module_dir.glob("*.ko*"), key=lambda item: item.name)
        if not module_files:
            raise ValueError("replacement kernel-module closure is empty")
        kernel.update(
            release=kernel_release,
            files=[
                {
                    "name": path.name,
                    "sha256": sha256_file(path),
                    "size": path.stat().st_size,
                }
                for path in module_files
            ],
        )
    return (
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
        + b"\n"
    )


def build_bundle(root: Path, manifest_bytes: bytes, output: Path) -> None:
    temporary = output.with_suffix(output.suffix + ".tmp")
    output.parent.mkdir(parents=True, exist_ok=True)
    with temporary.open("wb") as raw:
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=raw, compresslevel=1, mtime=0
        ) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|") as archive:
                info = tarfile.TarInfo("package-bundle.json")
                info.size = len(manifest_bytes)
                info.mode = 0o644
                normalized_tar_info(info)
                archive.addfile(info, io.BytesIO(manifest_bytes))
                for path in sorted(
                    (item for item in root.rglob("*") if item.name != "package-bundle.json"),
                    key=lambda item: item.relative_to(root).as_posix(),
                ):
                    arcname = path.relative_to(root).as_posix()
                    archive.add(
                        path,
                        arcname=arcname,
                        recursive=False,
                        filter=normalized_tar_info,
                    )
    os.replace(temporary, output)
    output.with_name(output.name + ".sha256").write_text(
        sha256_file(output) + "\n", encoding="ascii"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--wheel", required=True, type=Path)
    parser.add_argument("--storage-backend", required=True, type=Path)
    parser.add_argument("--storage-manifest", required=True, type=Path)
    parser.add_argument("--storage-license", required=True, type=Path)
    parser.add_argument(
        "--kernel-release",
        help="replace the source kernel closure with modules for this release",
    )
    parser.add_argument(
        "--kernel-module-dir",
        type=Path,
        help="flat directory of .ko/.ko.* files for --kernel-release",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    inputs = (
        args.source,
        args.wheel,
        args.storage_backend,
        args.storage_manifest,
        args.storage_license,
    )
    for path in inputs:
        if not path.is_file():
            raise SystemExit(f"required input does not exist: {path}")
    if bool(args.kernel_release) != bool(args.kernel_module_dir):
        raise SystemExit(
            "--kernel-release and --kernel-module-dir must be supplied together"
        )
    if args.kernel_module_dir is not None and not args.kernel_module_dir.is_dir():
        raise SystemExit(f"kernel module directory does not exist: {args.kernel_module_dir}")
    if args.output.resolve() == args.source.resolve():
        raise SystemExit("output must not replace the qualified source bundle")

    storage_build = validate_storage_build(
        args.storage_backend, args.storage_manifest, args.storage_license
    )
    with tempfile.TemporaryDirectory(prefix="ucloud-node-bundle-") as raw_work:
        work = Path(raw_work)
        bundle_root = work / "bundle"
        agent_root = work / "agent"
        bundle_root.mkdir()
        agent_root.mkdir()
        extract_tar(args.source, bundle_root)
        manifest = json.loads(
            (bundle_root / "package-bundle.json").read_text(encoding="utf-8")
        )
        validate_source_bundle(bundle_root, manifest)

        agent_archive = bundle_root / "runtime/agent/node-agent-runtime.tar"
        extract_tar(agent_archive, agent_root)
        replace_agent_package(agent_root, args.wheel)
        build_agent_archive(agent_root, agent_archive)

        shutil.copyfile(
            args.storage_backend, bundle_root / "runtime/storage-native/backend"
        )
        shutil.copyfile(
            args.storage_manifest,
            bundle_root / "runtime/storage-native/build-manifest.json",
        )
        shutil.copyfile(
            args.storage_license, bundle_root / "runtime/storage-native/LICENSE"
        )
        if args.kernel_release is not None and args.kernel_module_dir is not None:
            kernel_root = bundle_root / "runtime/kernel"
            shutil.rmtree(kernel_root)
            target_kernel_dir = kernel_root / args.kernel_release
            target_kernel_dir.mkdir(parents=True)
            for module in sorted(args.kernel_module_dir.glob("*.ko*")):
                if not module.is_file():
                    continue
                shutil.copyfile(module, target_kernel_dir / module.name)
        manifest_bytes = update_manifest(
            manifest,
            agent_archive=agent_archive,
            storage_backend=args.storage_backend,
            storage_manifest=args.storage_manifest,
            storage_license=args.storage_license,
            storage_build=storage_build,
            kernel_release=args.kernel_release,
            kernel_module_dir=(
                bundle_root / "runtime/kernel" / args.kernel_release
                if args.kernel_release is not None
                else None
            ),
        )
        build_bundle(bundle_root, manifest_bytes, args.output)

    print(f"bundle={args.output}")
    print(f"sha256={sha256_file(args.output)}")


if __name__ == "__main__":
    main()
