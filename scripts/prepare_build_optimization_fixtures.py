#!/usr/bin/env python3
r"""Prepare 48 fresh app edits using the frozen 2026-09-29 build-load inputs.

No network, dependency resolution or image builds. Only a new output directory
is written. The explicitly supplied generator is checked against the archived
checksum before loading it; importing a different local copy is not allowed.

Example on the gateway (stage the archive metadata there first):
  python3 prepare_build_optimization_fixtures.py \
    --generator /work/ucloud-sandboxes/build-load-20260929/build_load_fixtures.py \
    --frozen-root /work/ucloud-sandboxes/build-load-20260929 \
    --archive-root /work/ucloud-sandboxes/build-optimization-inputs-20260929 \
    --output-root /work/ucloud-sandboxes/build-optimization-fresh-20260929

The archive root must contain validation.json, fixture-manifests.json,
base-pins.json and locks/{node-base.json,python-base-resolved.txt} from
docs/benchmarks/build-load-2026-09-29. Override --node-lock/--python-lock if the
frozen server keeps those files outside its locks directory.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import stat


RECIPES = ("python-agent", "typescript-tools", "typescript-multistage")
VARIANTS = tuple(f"app-change-{index}" for index in range(29, 45))
ARCHIVE_PREFIX = "docs/benchmarks/build-load-2026-09-29/"
INPUT_NAMES = ("base-pins.json", "locks/node-base.json", "locks/python-base-resolved.txt")
MAX_CONTEXT_FILES = 6000
MAX_CONTEXT_BYTES = 16 * 1024**2


def fingerprint(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_CONTEXT_BYTES:
        raise ValueError(f"expected a bounded regular input file: {path}")
    data = path.read_bytes()
    return {"path": str(path), "bytes": len(data), "mode": stat.S_IMODE(info.st_mode),
            "sha256": hashlib.sha256(data).hexdigest()}


def write_json(path, value):
    with path.open("x", encoding="utf-8") as output:
        output.write(json.dumps(value, indent=2, sort_keys=True) + "\n")


def inventory(root):
    """Same context identity as the frozen generator, plus bounded file hashes."""
    files, total, digest = {}, 0, hashlib.sha256()
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            raise ValueError(f"fixture inputs must not contain symlinks: {path}")
        if path.is_dir():
            continue
        if relative == "fixture.json":
            continue
        if any(part in {"node_modules", ".git", "__pycache__"} for part in path.relative_to(root).parts):
            raise ValueError(f"unexpected generated/cache file in frozen fixture: {path}")
        value = fingerprint(path)
        total += value["bytes"]
        if total > MAX_CONTEXT_BYTES or len(files) >= MAX_CONTEXT_FILES:
            raise ValueError("fixture input exceeds bounded context size")
        files[relative] = value["sha256"]
        digest.update(relative.encode() + b"\0" + bytes.fromhex(value["sha256"]))
    return {"context_sha256": digest.hexdigest(), "context_files": len(files), "context_bytes": total,
            "files": files}


def load_generator(path):
    # Compile directly to avoid generating __pycache__ beside a frozen input.
    namespace = {"__name__": "_frozen_build_load_fixtures", "__file__": str(path)}
    exec(compile(path.read_bytes(), str(path), "exec"), namespace)
    if tuple(namespace.get("RECIPES", ())) != RECIPES or not callable(namespace.get("generate_context")):
        raise ValueError("frozen generator interface does not match the benchmark")
    return namespace["generate_context"]


def prepare(args):
    frozen = args.frozen_root.resolve(strict=True)
    archive = args.archive_root.resolve(strict=True)
    generator_path = args.generator.absolute()
    output = args.output_root.absolute()
    if output.exists() or output.is_symlink():
        raise ValueError("output root must not exist, including as a symlink")
    parent = output.parent.resolve(strict=True)
    output = parent / output.name
    if output.is_relative_to(frozen) or output.is_relative_to(archive):
        raise ValueError("output root must be outside both frozen and archived input roots")
    paths = {
        "base-pins.json": args.base_pins or frozen / "base-pins.json",
        "locks/node-base.json": args.node_lock or frozen / "locks/node-base.json",
        "locks/python-base-resolved.txt": args.python_lock or frozen / "locks/python-base-resolved.txt",
    }
    validation_path = archive / "validation.json"
    archive_manifests_path = archive / "fixture-manifests.json"
    validation = json.loads(validation_path.read_text())
    archived = json.loads(archive_manifests_path.read_text())
    checksums = validation["sha256"]
    source_checksums = {"generator": fingerprint(generator_path), "validation": fingerprint(validation_path),
                        "archived_fixture_manifests": fingerprint(archive_manifests_path)}
    if source_checksums["generator"]["sha256"] != checksums["scripts/build_load_fixtures.py"]:
        raise ValueError("explicit generator does not match the archived frozen generator checksum")
    frozen_bytes = {}
    for name, path in paths.items():
        actual, expected = fingerprint(path), fingerprint(archive / name)
        if actual["sha256"] != expected["sha256"] or actual["sha256"] != checksums[ARCHIVE_PREFIX + name]:
            raise ValueError(f"frozen input differs from archived bytes/checksum: {name}")
        source_checksums[name] = actual
        source_checksums["archive/" + name] = expected
        frozen_bytes[name] = path.read_bytes()
    pins = json.loads(frozen_bytes["base-pins.json"])["images"]
    for name in ("python:3.12-bookworm", "node:22-bookworm", "node:22-bookworm-slim"):
        if not re.fullmatch(re.escape(name) + r"@sha256:[0-9a-f]{64}", pins[name]):
            raise ValueError(f"missing immutable base pin: {name}")
    baseline, baseline_files = {}, {}
    for recipe in RECIPES:
        matches = [item for item in archived if item.get("recipe") == recipe and item.get("variant") == "app-change-1"]
        if len(matches) != 1:
            raise ValueError(f"archive must contain exactly one frozen baseline for {recipe}")
        expected = matches[0]
        root = frozen / "contexts" / recipe / "app-change-1"
        if root.is_symlink():
            raise ValueError("frozen context root must not be a symlink")
        current = inventory(root)
        manifest = json.loads((root / "fixture.json").read_text())
        for key in ("context_sha256", "context_files", "context_bytes"):
            if current[key] != expected[key] or manifest.get(key) != expected[key]:
                raise ValueError(f"frozen context no longer matches measured archive: {recipe}/{key}")
        base_name = "python:3.12-bookworm" if recipe == "python-agent" else "node:22-bookworm"
        if expected["base_ref"] != pins[base_name] or (recipe == "typescript-multistage" and expected["runtime_base_ref"] != pins["node:22-bookworm-slim"]):
            raise ValueError(f"archived fixture base does not match frozen pins: {recipe}")
        if type(expected["source_files"]) is not int or not 1 <= expected["source_files"] <= 5000:
            raise ValueError("invalid archived source file count")
        baseline[recipe], baseline_files[recipe] = expected, current
        source_checksums["baseline/" + recipe] = fingerprint(root / "fixture.json")
        for variant in VARIANTS:
            if (frozen / "contexts" / recipe / variant).exists() or any(item.get("recipe") == recipe and item.get("variant") == variant for item in archived):
                raise ValueError(f"requested fresh fixture already exists in frozen evidence: {recipe}/{variant}")
    generate = load_generator(generator_path)
    output.mkdir(mode=0o700)
    manifests = []
    try:
        (output / "locks").mkdir()
        for name, payload in frozen_bytes.items():
            with (output / name).open("xb") as target:
                target.write(payload)
        for variant in VARIANTS:
            for recipe in RECIPES:
                target = output / "contexts" / recipe / variant
                old = baseline[recipe]
                manifest = generate(target, recipe, variant, base_ref=old["base_ref"],
                    runtime_base_ref=old.get("runtime_base_ref"), source_files=old["source_files"],
                    node_lock=output / "locks/node-base.json" if recipe != "python-agent" else None,
                    python_lock=output / "locks/python-base-resolved.txt" if recipe == "python-agent" else None)
                actual = inventory(target)
                for key in ("context_sha256", "context_files", "context_bytes"):
                    if manifest[key] != actual[key]:
                        raise ValueError(f"generated context inventory mismatch: {recipe}/{variant}/{key}")
                previous_files = baseline_files[recipe]["files"]
                if actual["files"].keys() != previous_files.keys():
                    raise ValueError("fresh app edit changed the fixture file set")
                changed = [name for name in actual["files"] if actual["files"][name] != previous_files[name]]
                expected_revision = "src/agent/revision.py" if recipe == "python-agent" else "src/revision.ts"
                if changed != [expected_revision]:
                    raise ValueError(f"fresh app edit changed files beyond its revision: {recipe}/{variant}: {changed}")
                lock_name = "requirements.txt" if recipe == "python-agent" else "package-lock.json"
                lock_source = "locks/python-base-resolved.txt" if recipe == "python-agent" else "locks/node-base.json"
                if (target / lock_name).read_bytes() != frozen_bytes[lock_source]:
                    raise ValueError("fresh fixture lock bytes changed")
                manifest = dict(manifest, changed_files_from_frozen_baseline=changed,
                                unchanged_non_source_files=True,
                                source_file_sha256={name: actual["files"][name] for name in changed})
                manifests.append(manifest)
        # Recheck input bytes and full baseline contexts after generation.
        for name, before in source_checksums.items():
            if fingerprint(Path(before["path"])) != before:
                raise ValueError(f"input changed during generation: {name}")
        for recipe in RECIPES:
            if inventory(frozen / "contexts" / recipe / "app-change-1") != baseline_files[recipe]:
                raise ValueError(f"frozen baseline changed during generation: {recipe}")
        if len(manifests) != 48 or len({item["context_sha256"] for item in manifests}) != 48:
            raise ValueError("expected exactly 48 distinct fresh contexts")
        write_json(output / "fixture-manifests.json", manifests)
        write_json(output / "source-checksums.json", {"files": source_checksums,
            "frozen_baselines": baseline_files, "original_inputs_unchanged": True})
        receipt = {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
                   "output_root": str(output), "frozen_root": str(frozen), "archive_root": str(archive),
                   "generator": source_checksums["generator"], "recipes": list(RECIPES),
                   "variants": list(VARIANTS), "fixture_count": len(manifests),
                   "context_bytes": sum(item["context_bytes"] for item in manifests),
                   "context_files": sum(item["context_files"] for item in manifests),
                   "only_revision_source_changed": True, "original_inputs_unchanged": True,
                   "network_requests": 0, "builds_executed": 0,
                   "manifest_sha256": fingerprint(output / "fixture-manifests.json")["sha256"],
                   "source_checksums_sha256": fingerprint(output / "source-checksums.json")["sha256"]}
        write_json(output / "preparation-receipt.json", receipt)
        return receipt
    except Exception as exc:
        write_json(output / "preparation-failed.json", {"completed_fixtures": len(manifests),
            "exception_type": type(exc).__name__, "error": str(exc), "output_root": str(output)})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--generator", required=True, type=Path, help="Explicit frozen server-side generator path")
    parser.add_argument("--frozen-root", required=True, type=Path)
    parser.add_argument("--archive-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path, help="New sibling root; parent must already exist")
    parser.add_argument("--base-pins", type=Path)
    parser.add_argument("--node-lock", type=Path)
    parser.add_argument("--python-lock", type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
