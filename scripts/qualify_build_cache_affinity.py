#!/usr/bin/env python3
"""Bounded live proof using only invocation-owned BuildKit drivers/cache manifests.

Run manually on an owned idle Linux builder with the candidate package on
PYTHONPATH. Creates ten tiny cache exports, compares fresh recency/affinity
imports, checks source/ARG invalidation, and cleans only its own resources.
No global Docker prune, shared builder mutation, or registry GC is performed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tarfile
import time
from urllib.parse import urlsplit
from uuid import uuid4

try:
    from .analyze_buildkit_progress import parse_progress
except ImportError:
    from analyze_buildkit_progress import parse_progress


MARKER = "UCLOUD_AFFINITY_PROOF_EXECUTED"
MAX_LOG_BYTES = 4 * 1024 * 1024


def fixture(base_image, variant):
    dockerfile = (f"FROM {base_image}\nARG PROOF_ARG=original\n"
                  "COPY input.txt /proof-input.txt\n"
                  f"RUN printf '{MARKER}\\n' && "
                  "printf '%s\\n' \"$PROOF_ARG\" > /proof-arg.txt && "
                  "sha256sum /proof-input.txt > /proof-sha256.txt\n")
    return dockerfile.encode(), f"owned-affinity-variant-{variant}\n".encode()


def affinity_key(dockerfile, source, argument="original"):
    return hashlib.sha256(dockerfile + b"\0" + source + b"\0" + argument.encode()).hexdigest()


def verify_tar(path, source, argument):
    expected = {"proof-input.txt": source, "proof-arg.txt": (argument + "\n").encode(),
                "proof-sha256.txt": (hashlib.sha256(source).hexdigest() + "  /proof-input.txt\n").encode()}
    found = {}
    with tarfile.open(path, "r:*") as archive:
        for member in archive:
            name = member.name.removeprefix("./")
            if name not in expected:
                continue
            if name in found or not member.isfile() or member.size > 65536:
                raise ValueError("invalid or duplicate proof file in exported rootfs")
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError("proof file is not readable")
            found[name] = stream.read(65537)
    if found != expected:
        raise ValueError("exported proof files do not match source/ARG")
    return {name: hashlib.sha256(value).hexdigest() for name, value in sorted(found.items())}


def classify_run(log):
    progress = parse_progress(log)
    runs = [v for v in progress["vertices"] if v["category"] == "run"]
    executed = any(re.match(r"^#\d+ \d+(?:\.\d+)? " + MARKER + r"$", line)
                   for line in log.splitlines())
    return {"execution_marker_seen": executed,
            "run_cached_marker_seen": any("cached" in v["terminal_statuses_seen"] for v in runs),
            "completed_vertex_seconds_by_category": progress["completed_vertex_seconds_by_category"],
            "coverage": progress["coverage"],
            "log_sha256": hashlib.sha256(log.encode()).hexdigest(),
            "log_bytes": len(log.encode())}


def run(args):
    import tomllib  # This live Linux helper uses the builder's Python 3.11+.

    from ucloud_sandboxes.build_cache import RegistryBuildCache
    from ucloud_sandboxes.managed_registry import RegistryRequestError, normalize_manifest_digest
    from ucloud_sandboxes.vm_init import PINNED_BUILDKIT_IMAGE

    endpoint = urlsplit(args.registry_url)
    if (endpoint.scheme not in {"http", "https"} or not endpoint.netloc or
            endpoint.path not in {"", "/"} or endpoint.username or endpoint.password or
            endpoint.query or endpoint.fragment):
        raise ValueError("registry URL must be a plain HTTP(S) authority")
    if not re.fullmatch(r"[A-Za-z0-9./:_-]+@sha256:[0-9a-f]{64}", args.base_image):
        raise ValueError("base image must be pinned by digest")
    if not 120 <= args.timeout_seconds <= 1800:
        raise ValueError("overall deadline must be 120..1800 seconds")
    config = args.buildkit_config.resolve(strict=True)
    config_bytes = config.read_bytes()
    settings = tomllib.loads(config_bytes.decode())
    if settings.get("worker", {}).get("oci", {}).get("max-parallelism") != 4:
        raise ValueError("qualification requires the production four-solver limit")
    if settings.get("system", {}).get("maxRegistryConcurrency") != 4:
        raise ValueError("qualification requires the production four-registry-request limit")
    work = args.work_root.absolute()
    work.mkdir(mode=0o700, parents=False, exist_ok=False)
    invocation = uuid4().hex[:16]
    repository = "ucloud-build-cache-qual-" + invocation
    cache = RegistryBuildCache(f"{endpoint.netloc}/{repository}", registry_url=args.registry_url)
    owned_drivers = []
    owned_tags = []
    deadline = time.monotonic() + args.timeout_seconds
    receipt = {"schema_version": 1, "invocation": invocation, "complete": False,
               "started_at": datetime.now(timezone.utc).isoformat(),
               "repository": repository, "buildkit_image": PINNED_BUILDKIT_IMAGE,
               "base_image": args.base_image, "buildkit_config_sha256": hashlib.sha256(config_bytes).hexdigest(),
               "solver_parallelism": 4, "registry_concurrency": 4, "seeds": [], "trials": [],
               "cleanup": {"drivers": [], "manifests": [], "errors": []},
               "limits": ["Synthetic selection/semantic proof, not a production latency benchmark.",
                          "Single-recipe no-affinity selection is the historical newest-recipe plus recent fallback algorithm over the same frozen cache tags.",
                          "Proof affinity hashes are generated by this helper; runtime request identity propagation is covered by separate tests.",
                          "Registry manifest deletion unlinks owned references only; unreferenced blobs await normal GC."]}

    def save():
        (work / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")

    def command(arguments, label, *, cleanup=False):
        timeout = 60 if cleanup else min(180, deadline - time.monotonic())
        if timeout <= 0:
            raise TimeoutError("qualification overall deadline elapsed")
        with (work / (label + ".log")).open("wb") as output:
            result = subprocess.run([args.docker, *arguments], stdout=output,
                                    stderr=subprocess.STDOUT, timeout=timeout, check=False)
        if result.returncode:
            raise RuntimeError("owned BuildKit operation failed: " + label)

    def create_driver(label):
        name = f"ucloud-affinity-{invocation}-{label}"
        owned_drivers.append(name)
        receipt.setdefault("owned_driver_names", []).append(name)
        save()
        command(["buildx", "create", "--name", name, "--driver", "docker-container",
                 "--driver-opt", "image=" + PINNED_BUILDKIT_IMAGE,
                 "--driver-opt", "network=host", "--buildkitd-config", str(config)], label + "-create")
        command(["buildx", "inspect", name, "--bootstrap"], label + "-bootstrap")
        return name

    def remove_driver(name):
        command(["buildx", "rm", "--force", name], name + "-remove", cleanup=True)
        owned_drivers.remove(name)
        receipt["cleanup"]["drivers"].append({"name": name, "removed": True})

    def build(driver, label, variant, *, imports=(), export_ref="", argument="original"):
        dockerfile, source = fixture(args.base_image, variant)
        context = work / (label + "-context")
        context.mkdir(mode=0o700)
        (context / "Dockerfile").write_bytes(dockerfile)
        (context / "input.txt").write_bytes(source)
        archive = work / (label + ".tar")
        cmd = ["buildx", "build", "--builder", driver, "--progress=plain", "--provenance=false",
               "--build-arg", "PROOF_ARG=" + argument, "--output", "type=tar,dest=" + str(archive)]
        for ref in imports:
            cmd += ["--cache-from", "type=registry,ref=" + ref]
        if export_ref:
            tag = export_ref.rsplit(":", 1)[1]
            owned_tags.append(tag)
            receipt.setdefault("owned_tags", []).append(tag)
            save()
            cmd += ["--cache-to", "type=registry,ref=" + export_ref + ",mode=min,oci-mediatypes=true,image-manifest=true,ignore-error=false"]
        cmd.append(str(context))
        started = time.monotonic()
        command(cmd, label)
        elapsed = time.monotonic() - started
        log_path = work / (label + ".log")
        with log_path.open("rb") as handle:
            data = handle.read(MAX_LOG_BYTES + 1)
        if len(data) > MAX_LOG_BYTES:
            raise ValueError("proof log exceeds input bound")
        result = {"label": label, "wall_seconds": elapsed,
                  "source_sha256": hashlib.sha256(source).hexdigest(), "argument": argument,
                  "output_file_sha256": verify_tar(archive, source, argument),
                  "progress": classify_run(data.decode("utf-8", errors="replace"))}
        archive.unlink()
        return result

    failure = None
    save()
    try:
        seed_driver = create_driver("seed")
        dockerfile, first_source = fixture(args.base_image, 0)
        recipe = hashlib.sha256(dockerfile).hexdigest()
        exact_affinity = affinity_key(dockerfile, first_source)
        first_timestamp = None
        for index in range(10):
            if index == 1:
                # Real creation timestamps, without editing cache metadata. Only
                # variant 0 needs to precede the other nine unambiguously.
                while int(time.time()) <= first_timestamp:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("qualification deadline elapsed")
                    time.sleep(.05)
            _, source = fixture(args.base_image, index)
            plan = cache.prepare(recipe, affinity_key=affinity_key(dockerfile, source))
            if index == 0:
                first_timestamp = int(plan.export_ref.rsplit("-", 2)[1])
            result = build(seed_driver, f"seed-{index:02}", index, export_ref=plan.export_ref)
            tag = plan.export_ref.rsplit(":", 1)[1]
            digest = normalize_manifest_digest(cache.client.manifest_digest(repository, tag))
            if not digest:
                raise ValueError("seed cache digest is missing")
            receipt["seeds"].append({"index": index, "tag": tag, "manifest_digest": digest,
                                     "affinity_key": affinity_key(dockerfile, source), **result})
            save()
        remove_driver(seed_driver)
        baseline = cache.prepare(recipe)
        candidate = cache.prepare(recipe, affinity_key=exact_affinity)
        old_ref = f"{cache.repository_ref}:{receipt['seeds'][0]['tag']}"
        if (len(baseline.imports) != 8 or len(candidate.imports) != 8 or old_ref in baseline.imports
                or candidate.imports[0] != old_ref or not candidate.affinity_match):
            raise ValueError("frozen inventory does not prove an old exact hit outside baseline imports")
        inventory = {seed["tag"]: seed["manifest_digest"] for seed in receipt["seeds"]}
        receipt["selection"] = {"inventory": inventory, "exact_tag": receipt["seeds"][0]["tag"],
                                "baseline_import_tags": [ref.rsplit(":", 1)[1] for ref in baseline.imports],
                                "affinity_import_tags": [ref.rsplit(":", 1)[1] for ref in candidate.imports],
                                "affinity_match": candidate.affinity_match, "frozen_before_trials": True}
        save()
        baseline_driver = create_driver("baseline")
        baseline_result = build(baseline_driver, "baseline", 0, imports=baseline.imports)
        receipt["trials"].append(baseline_result)
        save()
        if not baseline_result["progress"]["execution_marker_seen"]:
            raise ValueError("baseline did not execute the target RUN; proof is inconclusive")
        remove_driver(baseline_driver)
        candidate_driver = create_driver("affinity")
        candidate_result = build(candidate_driver, "affinity", 0, imports=candidate.imports)
        receipt["trials"].append(candidate_result)
        save()
        if (candidate_result["progress"]["execution_marker_seen"] or
                not candidate_result["progress"]["run_cached_marker_seen"]):
            raise ValueError("affinity did not reuse the target RUN")
        if candidate_result["output_file_sha256"] != baseline_result["output_file_sha256"]:
            raise ValueError("baseline and affinity output bytes differ")
        for label, variant, argument in (("changed-source", 10, "original"), ("changed-arg", 0, "changed")):
            result = build(candidate_driver, label, variant, imports=candidate.imports, argument=argument)
            result["intentionally_stale_cache_imports"] = True
            receipt["trials"].append(result)
            save()
            if not result["progress"]["execution_marker_seen"]:
                raise ValueError("changed source/ARG did not invalidate the target RUN")
        # A final read confirms trials did not mutate the frozen registry inputs.
        if any(normalize_manifest_digest(cache.client.manifest_digest(repository, tag)) != digest
               for tag, digest in inventory.items()):
            raise ValueError("seed cache inventory changed during trials")
        receipt["proof_passed"] = True
    except BaseException as exc:
        receipt["error_type"] = type(exc).__name__
        failure = exc
    finally:
        for name in list(owned_drivers):
            try:
                remove_driver(name)
            except Exception as exc:
                receipt["cleanup"]["errors"].append({"resource": "owned_driver", "name": name, "error_type": type(exc).__name__})
        deleted = set()
        for tag in owned_tags:
            try:
                digest = normalize_manifest_digest(cache.client.manifest_digest(repository, tag))
                if not digest:
                    raise ValueError("owned cache cleanup requires a valid manifest digest")
                if digest not in deleted:
                    cache.client.delete_manifest(repository, digest)
                    deleted.add(digest)
                    receipt["cleanup"]["manifests"].append({"digest": digest, "deleted": True})
            except RegistryRequestError as exc:
                if exc.status_code != 404:
                    receipt["cleanup"]["errors"].append({"resource": "owned_cache_manifest", "error_type": type(exc).__name__})
            except Exception as exc:
                receipt["cleanup"]["errors"].append({"resource": "owned_cache_manifest", "error_type": type(exc).__name__})
        for tag in owned_tags:
            try:
                if cache.client.tag_exists(repository, tag):
                    receipt["cleanup"]["errors"].append({"resource": "owned_cache_tag_remaining", "tag": tag})
            except Exception as exc:
                receipt["cleanup"]["errors"].append({"resource": "owned_cache_tag_verification", "error_type": type(exc).__name__})
        receipt["cleanup"]["no_global_prune_or_gc"] = True
        receipt["complete"] = bool(receipt.get("proof_passed") and not receipt["cleanup"]["errors"])
        receipt["finished_at"] = datetime.now(timezone.utc).isoformat()
        save()
    print(json.dumps({"complete": receipt["complete"], "proof_passed": receipt.get("proof_passed", False),
                      "cleanup_errors": len(receipt["cleanup"]["errors"])}))
    if failure is not None or not receipt["complete"]:
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", required=True)
    parser.add_argument("--base-image", required=True, help="Small /bin/sh + sha256sum image, pinned by sha256 digest")
    parser.add_argument("--buildkit-config", type=Path, default=Path("/etc/ucloud-sandboxes/buildkit/buildkitd.toml"))
    parser.add_argument("--work-root", type=Path, required=True, help="New private directory on the owned idle builder")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--docker", default="docker")
    raise SystemExit(run(parser.parse_args()))


if __name__ == "__main__":
    main()
