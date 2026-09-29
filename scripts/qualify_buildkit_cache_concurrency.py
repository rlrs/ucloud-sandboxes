#!/usr/bin/env python3
"""Read-only registry diagnostic: serial/exact8/broad64 on private BuildKit stores.

Uses owned immutable fixture contexts and freezes the newest 64 live cache tags
before any solve. Does not export images/caches or modify the shared builder.
All created private drivers are removed in finally; raw logs stay in work-root.
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
               "arms": [], "owned_driver_names": [], "cleanup": [], "cleanup_errors": [],
               "limits": ["Cache-reuse diagnostic only: cacheonly exporter does not verify a runnable final image.",
                          "Plain progress can display shared concurrent vertices; execution counts are per-request observations, not unique worker executions.",
                          "Each arm starts with an independent empty BuildKit store; registry/host page caches may warm across serial arms.",
                          "Both concurrent arms use the same frozen newest-64 tag/digest inventory; tag aliases can share one manifest digest.",
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
        name = f"ucloud-cache-diag-{invocation}-{label}"
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
        started_snapshot = datetime.now(timezone.utc).isoformat()
        tags = cache._tags(deadline=min(deadline, time.monotonic() + 30))
        if len(tags) > 256:
            raise ValueError("live cache inventory exceeds diagnostic bound")
        now = int(time.time())
        eligible = [(owned.created_at, tag, owned) for tag in tags if (owned := _parse_owned_tag(tag)) is not None
                    and now - cache.max_age_seconds <= owned.created_at <= now]
        eligible.sort(reverse=True)
        newest = eligible[:64]
        if len(newest) != 64:
            raise ValueError("diagnostic requires 64 eligible cache tags")
        inventory = []
        by_tag = {}
        for _, tag, owned in newest:
            if time.monotonic() >= deadline:
                raise TimeoutError("cache snapshot deadline elapsed")
            digest = normalize_manifest_digest(cache.client.manifest_digest(cache.repository, tag))
            if not digest:
                raise ValueError("cache snapshot has an invalid digest")
            row = {"tag": tag, "manifest_digest": digest, "recipe_hint": owned.recipe,
                   "affinity_hint": owned.affinity, "created_at_unix": owned.created_at}
            inventory.append(row)
            by_tag[tag] = row
        # Only the tag-list method is replaced, on this private diagnostic
        # instance. prepare now chooses from the same immutable inventory.
        cache._tags = lambda **_kwargs: [row["tag"] for row in inventory]
        common_refs = tuple(f"{cache.repository_ref}@{row['manifest_digest']}" for row in inventory)
        for case in cases:
            # Runtime selection consumes only the first 128 bits of this hint.
            plan = cache.prepare(case["recipe"], affinity_key=case["affinity_hint"] + "0" * 32)
            if not plan.affinity_match or len(plan.imports) != 8:
                raise ValueError("latest64 inventory lacks an exact cache for a selected context")
            selected = [ref.rsplit(":", 1)[1] for ref in plan.imports]
            case["exact_tag"] = selected[0]
            case["exact8_tags"] = selected
            case["exact8_refs"] = tuple(f"{cache.repository_ref}@{by_tag[tag]['manifest_digest']}" for tag in selected)
            if any(ref not in common_refs for ref in case["exact8_refs"]):
                raise ValueError("exact8 is not a subset of the common broad inventory")
        receipt["snapshot"] = {"started_at": started_snapshot, "finished_at": datetime.now(timezone.utc).isoformat(),
                               "tags": inventory, "unique_manifest_digests": len(set(common_refs)),
                               "atomic_registry_snapshot": False}
        receipt["cases"] = [{key: case[key] for key in ("index", "variant", "context_sha256", "affinity_hint", "exact_tag", "exact8_tags")}
                            for case in cases]
        save()
        for arm, members, concurrency in (("serial-exact", cases[:1], 1), ("concurrent-exact8", cases, 4), ("concurrent-broad64", cases, 4)):
            driver = create_driver(arm)
            report = {"name": arm, "concurrency": concurrency, "records": []}
            receipt["arms"].append(report)
            save()
            started = time.monotonic()
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures = [pool.submit(solve, driver, arm, case,
                    (case["exact8_refs"][:1] if arm == "serial-exact" else
                     case["exact8_refs"] if arm == "concurrent-exact8" else common_refs)) for case in members]
                for future in as_completed(futures):
                    report["records"].append(future.result())
                    save()
            report["wall_seconds"] = time.monotonic() - started
            report["records"].sort(key=lambda row: row["index"])
            report["app_run_execution_observations"] = sum(r["app_run_execution_observed"] for r in report["records"])
            report["app_run_cached_observations"] = sum(r["app_run_cached_marker_seen"] for r in report["records"])
            report["import_error_vertices"] = sum(r["import_error_vertices"] for r in report["records"])
            remove_driver(driver)
            save()
        # HEAD only: verify none of the frozen inputs disappeared or retargeted.
        receipt["snapshot_still_matches"] = all(normalize_manifest_digest(cache.client.manifest_digest(cache.repository, row["tag"])) == row["manifest_digest"]
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
    parser.add_argument("--buildkit-config", type=Path, default=Path("/etc/ucloud-sandboxes/buildkit/buildkitd.toml"))
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--docker", default="docker")
    raise SystemExit(run(parser.parse_args()))


if __name__ == "__main__":
    main()
