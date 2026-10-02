#!/usr/bin/env python3
"""Verify frozen contexts and optionally run one bounded 48-build comparison.

The default only verifies inputs and prints the command. --run uses the existing
server-local build harness; its credentials never leave the gateway. This tool
does not deploy code, change fleet size, prune caches, or regenerate dependencies.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib
import inspect
import json
from pathlib import Path
import re
import subprocess
import sys

RECIPES = ("python-agent", "typescript-tools", "typescript-multistage")
VARIANTS = tuple("app-change-" + str(index) for index in range(5, 21))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_contexts(source_root, inventory_path, expected_inventory_sha256, *, recipes=RECIPES, variants=VARIANTS):
    if digest(inventory_path) != expected_inventory_sha256:
        raise ValueError("Frozen fixture inventory SHA256 differs")
    frozen = {(item["recipe"], item["variant"]): item for item in json.loads(inventory_path.read_text())}
    verified = []
    for recipe in recipes:
        for variant in variants:
            expected = frozen[(recipe, variant)]
            root = source_root / "contexts" / recipe / variant
            actual_manifest = json.loads((root / "fixture.json").read_text())
            # Context paths are provenance, not archive content; all execution
            # inputs and expected runtime results must remain identical.
            if ({key: value for key, value in actual_manifest.items() if key != "context_path"}
                    != {key: value for key, value in expected.items() if key != "context_path"}):
                raise ValueError("Fixture manifest changed: " + recipe + "/" + variant)
            inventory, count, size = hashlib.sha256(), 0, 0
            for path in sorted(root.rglob("*")):
                relative = path.relative_to(root)
                if path.is_symlink():
                    raise ValueError("Frozen contexts must not introduce symlinks")
                if not path.is_file() or relative.as_posix() == "fixture.json" or any(
                        part in {"node_modules", ".git", "__pycache__"} for part in relative.parts):
                    continue
                payload = path.read_bytes()
                inventory.update(relative.as_posix().encode() + b"\0" + hashlib.sha256(payload).digest())
                count += 1
                size += len(payload)
            observed = {"context_sha256": inventory.hexdigest(), "context_files": count, "context_bytes": size}
            if any(observed[key] != expected[key] for key in observed):
                raise ValueError("Frozen context bytes changed: " + recipe + "/" + variant)
            verified.append({"recipe": recipe, "variant": variant, **observed})
    return verified


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path("/work/ucloud-sandboxes/build-load-20260929"))
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--fixture-manifests", type=Path, required=True)
    parser.add_argument("--inventory-sha256", default="45efcdbd714d7fbc6748b26f7fcdc29b4b18d1f4117baac0feb8af24e32505a7")
    parser.add_argument("--harness", type=Path, default=Path(__file__).with_name("live_build_load_benchmark.py"))
    parser.add_argument("--harness-sha256", required=True)
    parser.add_argument("--phase", required=True, help="Unique phase name; never reuse image IDs")
    parser.add_argument("--candidate", choices=("B0-baseline", "B1-selective", "B2-selective-and-scheduling"), required=True)
    parser.add_argument("--artifact-sha256", required=True, help="Declared deployment wheel digest; runtime source hashes are also recorded")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9-]{1,30}", args.phase):
        raise ValueError("Use a unique phase of 1..30 lowercase letters, digits, or hyphens")
    for value in (args.inventory_sha256, args.harness_sha256, args.artifact_sha256):
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("Expected a lowercase SHA256 digest")
    if digest(args.harness) != args.harness_sha256:
        raise ValueError("Frozen harness SHA256 differs")
    contexts = verify_contexts(args.source_root, args.fixture_manifests, args.inventory_sha256)
    args.output_root = args.output_root.absolute()
    command = [sys.executable, str(args.harness.absolute()), "--root", str(args.output_root), "build",
               "--phase", args.phase, "--count", "48", "--concurrency", "48", "--variants", *VARIANTS,
               "--timeout", "1200", "--expected-builders", "4"]
    receipt = {"phase": args.phase, "candidate": args.candidate, "verified_contexts": contexts,
               "inventory_sha256": args.inventory_sha256, "harness_sha256": args.harness_sha256,
               "declared_artifact_sha256": args.artifact_sha256, "command": command,
               "count": 48, "concurrency": 48, "expected_builders": 4, "ran": False}
    if not args.run:
        print(json.dumps(receipt, indent=2))
        return
    args.output_root.mkdir(parents=True, exist_ok=True)
    contexts_link = args.output_root / "contexts"
    if not contexts_link.exists() and not contexts_link.is_symlink():
        contexts_link.symlink_to((args.source_root / "contexts").resolve(), target_is_directory=True)
    if contexts_link.resolve() != (args.source_root / "contexts").resolve():
        raise ValueError("Output root already names different contexts")
    output = args.output_root / (args.phase + ".qualification.json")
    if output.exists() or (args.output_root / args.phase).exists():
        raise ValueError("Phase already exists; do not reuse image IDs or replace evidence")
    receipt["gateway_imported_source_sha256"] = {}
    # The gateway's build tagging and digest protection live in gateway/ (C6.1).
    for name in ("environment_builder", "control_plane", "gateway.registry_refs",
                 "gateway.image_resolution", "node_agent", "images", "build_cache"):
        module = importlib.import_module("ucloud_sandboxes." + name)
        receipt["gateway_imported_source_sha256"][name] = hashlib.sha256(inspect.getsource(module).encode()).hexdigest()
    receipt["started_at"] = datetime.now(timezone.utc).isoformat()
    output.write_text(json.dumps(receipt, indent=2) + "\n")
    try:
        result = subprocess.run(command, timeout=1500, check=False)
        receipt.update(ran=True, returncode=result.returncode)
    finally:
        receipt["finished_at"] = datetime.now(timezone.utc).isoformat()
        output.write_text(json.dumps(receipt, indent=2) + "\n")
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
