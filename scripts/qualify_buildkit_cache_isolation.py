#!/usr/bin/env python3
"""Second read-only diagnostic: fan-in versus shared-daemon concurrency.

Reuses the first diagnostic's frozen tag/digest inventory and exact imports.
Runs serial exact8 for an R1 hit and miss, shared-daemon concurrent exact1, and four isolated exact1
drivers with one exclusive sequential job stream each. No exports or shared
builder mutation. Only invocation-owned drivers are removed in finally.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import time
from urllib.parse import urlsplit
from uuid import uuid4

try:
    from .analyze_buildkit_progress import parse_progress
except ImportError:
    from analyze_buildkit_progress import parse_progress


def context_identity(root):
    digest = hashlib.sha256()
    count = size = 0
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if path.is_symlink():
            raise ValueError("fixture context may not contain symlinks")
        if not path.is_file() or relative.as_posix() == "fixture.json" or any(
                part in {"node_modules", ".git", "__pycache__"} for part in relative.parts):
            continue
        payload = path.read_bytes()
        count += 1
        size += len(payload)
        if count > 6000 or size > 16 * 1024**2:
            raise ValueError("fixture context exceeds proof bound")
        digest.update(relative.as_posix().encode() + b"\0" + hashlib.sha256(payload).digest())
    return digest.hexdigest()


def application_observation(progress):
    vertices = [v for v in progress["vertices"] if v.get("fixture_activity") == "node_lint_build_test_smoke"]
    # A vertex can both materialize an input and execute. Its numeric command
    # output remains execution evidence even when the parser labels it mixed.
    executed = any(v["status"] == "done" and v["vertex_output_lines"] > 0 for v in vertices)
    cached = any("cached" in v["terminal_statuses_seen"] for v in vertices)
    if not vertices or not (executed or cached):
        raise ValueError("application cache/execution observation is incomplete")
    return vertices, executed, cached


def run(args):
    import tomllib

    from ucloud_sandboxes.build_cache import RegistryBuildCache, _parse_owned_tag
    from ucloud_sandboxes.managed_registry import normalize_manifest_digest
    from ucloud_sandboxes.vm_init import PINNED_BUILDKIT_IMAGE

    if not 120 <= args.timeout_seconds <= 1800:
        raise ValueError("deadline must be 120..1800 seconds")
    endpoint = urlsplit(args.registry_url)
    if (endpoint.scheme not in {"http", "https"} or not endpoint.netloc or endpoint.path not in {"", "/"}
            or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment):
        raise ValueError("plain HTTP(S) registry authority required")
    cache = RegistryBuildCache(args.cache_ref, registry_url=args.registry_url)
    if cache.authority != endpoint.netloc:
        raise ValueError("registry and cache authorities differ")
    config = args.buildkit_config.resolve(strict=True)
    raw_config = config.read_bytes()
    settings = tomllib.loads(raw_config.decode())
    if (settings.get("worker", {}).get("oci", {}).get("max-parallelism") != 4 or
            settings.get("system", {}).get("maxRegistryConcurrency") != 4):
        raise ValueError("production solver/registry concurrency must both be four")
    source = json.loads(args.seed_selection.read_text())
    if source.get("phase") != "affinity-seed" or source.get("repository") != cache.repository:
        raise ValueError("seed receipt does not identify this cache repository")
    frozen_bytes = args.frozen_receipt.read_bytes()
    frozen = json.loads(frozen_bytes)
    if (not frozen.get("complete") or not frozen.get("snapshot_still_matches") or
            frozen.get("repository") != cache.repository or frozen.get("buildkit_image") != PINNED_BUILDKIT_IMAGE or
            frozen.get("buildkit_config_sha256") != hashlib.sha256(raw_config).hexdigest()):
        raise ValueError("first diagnostic must be complete with the same registry/BuildKit/configuration")
    cases = []
    for variant in range(5, 17):
        matches = [row for row in source["records"] if row["recipe"] == "typescript-tools" and
                   row["variant"] == f"app-change-{variant}"]
        if len(matches) != 1:
            raise ValueError("expected one seed record per frozen case")
        row = matches[0]
        owned = _parse_owned_tag(row.get("export_tag") or "")
        if owned is None or not owned.affinity or not row.get("export_tag_uniquely_resolved"):
            raise ValueError("seed affinity hint is missing or ambiguous")
        context = (args.contexts_root / row["variant"]).resolve(strict=True)
        if context_identity(context) != row["context_sha256"]:
            raise ValueError("copied fixture context differs from frozen seed")
        recipe = hashlib.sha256((context / "Dockerfile").read_bytes()).hexdigest()
        if hashlib.sha256(recipe.encode()).hexdigest()[:16] != owned.recipe:
            raise ValueError("Dockerfile differs from the seed recipe hint")
        cases.append({"index": row["index"], "variant": row["variant"], "recipe": recipe,
                      "context": context, "context_sha256": row["context_sha256"],
                      "affinity_hint": owned.affinity, "image_id": row["image_id"]})
    work = args.work_root.absolute()
    work.mkdir(mode=0o700, parents=False, exist_ok=False)
    deadline = time.monotonic() + args.timeout_seconds
    invocation = uuid4().hex[:16]
    drivers = []
    receipt = {"schema_version": 1, "complete": False, "invocation": invocation,
               "started_at": datetime.now(timezone.utc).isoformat(), "repository": cache.repository,
               "buildkit_image": PINNED_BUILDKIT_IMAGE, "buildkit_config_sha256": hashlib.sha256(raw_config).hexdigest(),
               "seed_selection_sha256": hashlib.sha256(args.seed_selection.read_bytes()).hexdigest(),
               "frozen_receipt_sha256": hashlib.sha256(frozen_bytes).hexdigest(),
               "arms": [], "owned_driver_names": [], "cleanup": [], "cleanup_errors": [],
               "limits": ["Cache-reuse diagnostic only: cacheonly exporter does not verify a runnable final image.",
                          "Plain progress can display shared concurrent vertices; execution counts are per-request observations, not unique worker executions.",
                          "Each arm starts with an independent empty BuildKit store; registry/host page caches may warm across serial arms.",
                          "All arms reuse the first diagnostic's exact immutable inputs; no live recency reselection.",
                          "The four-driver arm has one sequential job stream per driver; each driver starts empty but can warm across its three jobs.",
                          "No registry cache/image exports, registry deletion, shared-builder mutation, or global prune."]}

    def save():
        (work / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")

    def command(cmd, label, *, cleanup=False):
        timeout = 60 if cleanup else min(180, deadline - time.monotonic())
        if timeout <= 0:
            raise TimeoutError("diagnostic overall deadline elapsed")
        with (work / (label + ".log")).open("wb") as output:
            result = subprocess.run([args.docker, *cmd], stdout=output, stderr=subprocess.STDOUT,
                                    timeout=timeout, check=False)
        if result.returncode:
            raise RuntimeError("owned diagnostic operation failed: " + label)

    def create_driver(label):
        name = f"ucloud-cache-iso-{invocation}-{label}"
        drivers.append(name)
        receipt["owned_driver_names"].append(name)
        save()
        command(["buildx", "create", "--name", name, "--driver", "docker-container",
                 "--driver-opt", "image=" + PINNED_BUILDKIT_IMAGE, "--driver-opt", "network=host",
                 "--buildkitd-config", str(config)], label + "-create")
        command(["buildx", "inspect", name, "--bootstrap"], label + "-bootstrap")
        return name

    def remove_driver(name):
        command(["buildx", "rm", "--force", name], name + "-remove", cleanup=True)
        drivers.remove(name)
        receipt["cleanup"].append({"driver": name, "removed": True})

    def solve(driver, arm, case, imports):
        label = arm + "-" + case["variant"]
        cmd = ["buildx", "build", "--builder", driver, "--progress=plain", "--provenance=false",
               "--label", "ucloud-sandboxes.image=true", "--label", "ucloud-sandboxes.image-id=" + case["image_id"],
               "--output=type=cacheonly"]
        for ref in imports:
            cmd += ["--cache-from", "type=registry,ref=" + ref]
        cmd.append(str(case["context"]))
        started = time.monotonic()
        command(cmd, label)
        elapsed = time.monotonic() - started
        with (work / (label + ".log")).open("rb") as stream:
            raw = stream.read(4 * 1024**2 + 1)
        if len(raw) > 4 * 1024**2:
            raise ValueError("diagnostic log exceeds bounded input")
        progress = parse_progress(raw.decode("utf-8", errors="replace"))
        app, executed, cached = application_observation(progress)
        return {"index": case["index"], "variant": case["variant"], "context_sha256": case["context_sha256"],
                "driver": driver,
                "wall_seconds": elapsed, "import_refs": list(imports), "log_sha256": hashlib.sha256(raw).hexdigest(),
                "log_bytes": len(raw), "app_run_execution_observed": executed,
                "app_run_cached_marker_seen": cached,
                "application_vertices": app,
                "copy_vertices": [v for v in progress["vertices"] if v["category"] == "copy"],
                "import_error_vertices": sum(v["category"] == "cache_import" and v["status"] == "error" for v in progress["vertices"]),
                "completed_vertex_seconds_by_category": progress["completed_vertex_seconds_by_category"],
                "coverage": progress["coverage"]}

    save()
    try:
        inventory = frozen["snapshot"]["tags"]
        if len(inventory) != 64:
            raise ValueError("first diagnostic must contain exactly64 inventory tags")
        by_tag = {}
        for row in inventory:
            if time.monotonic() >= deadline:
                raise TimeoutError("cache snapshot deadline elapsed")
            digest = normalize_manifest_digest(row["manifest_digest"])
            owned = _parse_owned_tag(row["tag"])
            if not digest or owned is None or row["tag"] in by_tag:
                raise ValueError("invalid or duplicate frozen inventory item")
            if normalize_manifest_digest(cache.client.manifest_digest(cache.repository, digest)) != digest:
                raise ValueError("frozen cache manifest is no longer available")
            by_tag[row["tag"]] = row
        previous_cases = {case["index"]: case for case in frozen["cases"]}
        if len(previous_cases) != 12:
            raise ValueError("first diagnostic must contain12 unique cases")
        for case in cases:
            previous = previous_cases[case["index"]]
            if any(previous[key] != case[key] for key in ("variant", "context_sha256", "affinity_hint")):
                raise ValueError("case inputs differ from the first diagnostic")
            selected = previous["exact8_tags"]
            if len(selected) != 8 or len(set(selected)) != 8 or any(tag not in by_tag for tag in selected):
                raise ValueError("invalid frozen eight-cache plan")
            exact = _parse_owned_tag(selected[0])
            if (selected[0] != previous["exact_tag"] or exact.affinity != case["affinity_hint"] or
                    exact.recipe != hashlib.sha256(case["recipe"].encode()).hexdigest()[:16]):
                raise ValueError("first frozen import does not match the exact case")
            case["exact_tag"] = selected[0]
            case["exact8_tags"] = selected
            case["exact8_refs"] = tuple(f"{cache.repository_ref}@{by_tag[tag]['manifest_digest']}" for tag in selected)
        receipt["snapshot"] = frozen["snapshot"]
        receipt["frozen_inputs_available_before_arms"] = True
        receipt["cases"] = [{key: case[key] for key in ("index", "variant", "context_sha256", "affinity_hint", "exact_tag", "exact8_tags")}
                            for case in cases]
        save()
        for arm, members, concurrency, driver_count in (("serial-exact8", cases[:1], 1, 1),
                ("serial-exact8-missed", cases[2:3], 1, 1),
                ("concurrent-exact1", cases, 4, 1), ("isolated-exact1", cases, 4, 4)):
            arm_drivers = [create_driver(f"{arm}-{slot}") for slot in range(driver_count)]
            report = {"name": arm, "concurrency": concurrency, "driver_count": driver_count, "records": []}
            receipt["arms"].append(report)
            save()
            started = time.monotonic()
            def exclusive_slot(slot, driver):
                # This one future owns a daemon for its complete sequential
                # stream, so no two solves can overlap on the same driver.
                return [solve(driver, arm, case, case["exact8_refs"][:1]) for case in members[slot::4]]

            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                if driver_count == 4:
                    futures = [pool.submit(exclusive_slot, slot, driver) for slot, driver in enumerate(arm_drivers)]
                else:
                    futures = [pool.submit(solve, arm_drivers[0], arm, case,
                        case["exact8_refs"] if arm.startswith("serial-exact8") else case["exact8_refs"][:1]) for case in members]
                for future in as_completed(futures):
                    value = future.result()
                    report["records"].extend(value if isinstance(value, list) else [value])
                    save()
            report["wall_seconds"] = time.monotonic() - started
            report["records"].sort(key=lambda row: row["index"])
            report["app_run_execution_observations"] = sum(r["app_run_execution_observed"] for r in report["records"])
            report["app_run_cached_observations"] = sum(r["app_run_cached_marker_seen"] for r in report["records"])
            report["import_error_vertices"] = sum(r["import_error_vertices"] for r in report["records"])
            for driver in arm_drivers:
                remove_driver(driver)
            save()
        # Immutable digest availability, independent of tag maintenance.
        receipt["snapshot_still_matches"] = all(normalize_manifest_digest(cache.client.manifest_digest(cache.repository, row["manifest_digest"])) == row["manifest_digest"]
                                                for row in inventory)
        receipt["diagnostic_finished"] = True
    except BaseException as exc:
        receipt["error_type"] = type(exc).__name__
    finally:
        for driver in list(drivers):
            try:
                remove_driver(driver)
            except Exception as exc:
                receipt["cleanup_errors"].append({"driver": driver, "error_type": type(exc).__name__})
        receipt["complete"] = bool(receipt.get("diagnostic_finished") and receipt.get("snapshot_still_matches") and
                                   not receipt["cleanup_errors"] and not any(a.get("import_error_vertices", 0) for a in receipt["arms"]))
        receipt["finished_at"] = datetime.now(timezone.utc).isoformat()
        save()
    print(json.dumps({"complete": receipt["complete"], "arms": [{key: arm[key] for key in (
        "name", "wall_seconds", "app_run_execution_observations", "app_run_cached_observations") if key in arm} for arm in receipt["arms"]]}))
    return 0 if receipt["complete"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", required=True)
    parser.add_argument("--cache-ref", required=True)
    parser.add_argument("--contexts-root", type=Path, required=True)
    parser.add_argument("--seed-selection", type=Path, required=True)
    parser.add_argument("--frozen-receipt", type=Path, required=True, help="Completed R1 diagnostic receipt; its exact imports are replayed")
    parser.add_argument("--buildkit-config", type=Path, default=Path("/etc/ucloud-sandboxes/buildkit/buildkitd.toml"))
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--docker", default="docker")
    raise SystemExit(run(parser.parse_args()))


if __name__ == "__main__":
    main()
