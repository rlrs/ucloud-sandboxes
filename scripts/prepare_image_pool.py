#!/usr/bin/env python3
"""Prepare pinned OCI images offline, with resumable receipts and storage admission.

Run on the gateway as its service account. The input is a schema-1 JSON plan
with an images array: source, families, task_rows. Public Docker Hub, GHCR, and
Microsoft images are supported; unresolved aliases never become ready.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
from pathlib import Path
import re
import sys
import threading
import time
from urllib import error as urlerror, parse, request
from uuid import uuid4


ACCEPT = ",".join(("application/vnd.oci.image.index.v1+json",
                   "application/vnd.docker.distribution.manifest.list.v2+json",
                   "application/vnd.oci.image.manifest.v1+json",
                   "application/vnd.docker.distribution.manifest.v2+json"))
GIB = 1024 ** 3


def retry_delay(headers, attempt, now):
    """Honor registry cooldowns, including HTTP-date Retry-After values."""
    fallback = min(900, 60 * 2 ** attempt)
    value = headers.get("Retry-After", "")
    try:
        delay = float(value)
    except ValueError:
        try:
            delay = parsedate_to_datetime(value).timestamp() - now
        except (ValueError, TypeError, OverflowError):
            delay = fallback
    return max(fallback, delay)


class SourceResolver:
    """Serialize each public host and persist cooldowns across coordinators."""

    def __init__(self, root, *, resolve=None, clock=time.time, sleep=time.sleep):
        self.root = root
        self.resolve = resolve or resolve_source
        self.clock = clock
        self.sleep = sleep

    def __call__(self, source):
        host = registry_parts(source)[0]
        path = self.root / ("source-" + host + ".json")
        deadline = self.clock() + 3600
        attempt = 0
        while True:
            with path.with_suffix(".lock").open("a") as handle:
                fcntl.flock(handle, fcntl.LOCK_EX)
                now = self.clock()
                state = json.loads(path.read_text()) if path.exists() else {}
                delay = max(0, state.get("next_request_at", 0) - now)
                if delay <= 0:
                    try:
                        result = self.resolve(source)
                    except urlerror.HTTPError as error:
                        if error.code not in {429, 502, 503, 504}:
                            raise
                        # All threads/processes share the same upstream quota.
                        # A different waiter must not reset exponential backoff.
                        failures = max(attempt, state.get("failures", 0))
                        delay = retry_delay(error.headers, failures, self.clock())
                        limits = {key: error.headers.get(key) for key in ("ratelimit-limit", "ratelimit-remaining")
                                  if error.headers.get(key) is not None}
                        error.close()
                        attempt += 1
                        save(path, {"next_request_at": self.clock() + delay, "failures": failures + 1,
                                    "limits": limits})
                        print(json.dumps({"registry": host, "status": "backoff", "seconds": delay}), flush=True)
                    else:
                        # Avoid a burst of token/manifest requests after each
                        # completion; builds proceed independently of this lock.
                        save(path, {"next_request_at": self.clock() + 1, "failures": 0})
                        return result
            if self.clock() + delay >= deadline or attempt >= 6:
                raise RuntimeError("deferred: public registry cooldown for " + host)
            self.sleep(min(delay, 60))


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def recover_catalog(root):
    path = root / "catalog.json"
    catalog = json.loads(path.read_text()) if path.exists() else {"schema": 1, "images": {}}
    results = root / "results"
    results.mkdir(exist_ok=True)
    for path in results.glob("*.json"):
        item = json.loads(path.read_text())
        if path.stem != hashlib.sha256(item["source"].encode()).hexdigest():
            raise ValueError("result journal identity mismatch")
        catalog["images"][item["source"]] = item
    return catalog


def journal_result(root, result):
    key = hashlib.sha256(result["source"].encode()).hexdigest()
    save(root / "results" / (key + ".json"), result)


def source_parts(source):
    source = source.removeprefix("docker.io/").removeprefix("registry-1.docker.io/")
    if not re.fullmatch(r"[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9_.-]+)?(?:@sha256:[a-f0-9]{64})?", source):
        raise ValueError("unsupported Docker Hub source")
    name, _, digest = source.partition("@")
    repository, tag = name.rsplit(":", 1) if ":" in name else (name, "latest")
    if "/" not in repository:
        repository = "library/" + repository
    elif "." in repository.split("/")[0] or len(repository.split("/")) != 2:
        raise ValueError("expected a public Docker Hub repository, not a platform alias")
    return repository, digest or tag


def public_registry_headers(host, repository):
    headers = {"Accept": ACCEPT}
    if host in {"docker.io", "ghcr.io"}:
        auth = "https://auth.docker.io/token" if host == "docker.io" else "https://ghcr.io/token"
        service = "registry.docker.io" if host == "docker.io" else host
        query = parse.urlencode({"service": service, "scope": f"repository:{repository}:pull"})
        with request.urlopen(auth + "?" + query, timeout=60) as response:
            token = json.load(response)["token"]
        headers["Authorization"] = "Bearer " + token
    return headers


def resolve_source(source):
    host, repository, selector = registry_parts(source)
    headers = public_registry_headers(host, repository)

    def get(kind, ref):
        endpoint = "registry-1.docker.io" if host == "docker.io" else host
        url = f"https://{endpoint}/v2/{repository}/{kind}/{ref}"
        with request.urlopen(request.Request(url, headers=headers), timeout=120) as response:
            content = response.read(16 * 1024 * 1024 + 1)
            if len(content) > 16 * 1024 * 1024:
                raise ValueError("upstream image metadata is too large")
            digest = "sha256:" + hashlib.sha256(content).hexdigest()
            if ref.startswith("sha256:") and digest != ref:
                raise ValueError("registry content digest mismatch")
            if response.headers.get("Docker-Content-Digest", digest) != digest:
                raise ValueError("registry digest header mismatch")
            return json.loads(content), digest, content.decode("utf-8")

    manifest, digest, manifest_json = get("manifests", selector)
    if "manifests" in manifest:
        matches = [m for m in manifest["manifests"] if m.get("platform", {}).get("os") == "linux"
                   and m.get("platform", {}).get("architecture") == "amd64"]
        if len(matches) != 1:
            raise ValueError("expected exactly one linux/amd64 image")
        manifest, digest, manifest_json = get("manifests", matches[0]["digest"])
    config, _, config_json = get("blobs", manifest["config"]["digest"])
    if config.get("os") != "linux" or config.get("architecture") != "amd64":
        raise ValueError("source is not linux/amd64")
    if len(manifest["layers"]) != len(config["rootfs"]["diff_ids"]):
        raise ValueError("layer and diff-id counts differ")
    return {"reference": host + "/" + repository + "@" + digest,
            "compressed_bytes": sum(layer["size"] for layer in manifest["layers"]),
            "layer_count": len(manifest["layers"]), "layers": manifest["layers"],
            "diff_ids": config["rootfs"]["diff_ids"], "onbuild": (config.get("config") or {}).get("OnBuild") or [],
            "manifest_json": manifest_json, "config_json": config_json}


def registry_parts(source):
    for host in ("ghcr.io", "mcr.microsoft.com"):
        if source.startswith(host + "/"):
            remainder = source[len(host) + 1:]
            if not re.fullmatch(r"[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9_.-]+)?(?:@sha256:[a-f0-9]{64})?", remainder):
                raise ValueError("invalid public registry reference")
            name, _, digest = remainder.partition("@")
            repository, tag = name.rsplit(":", 1) if ":" in name else (name, "latest")
            return host, repository, digest or tag
    repository, selector = source_parts(source)
    return "docker.io", repository, selector


def image_recipe(reference, preparation="source"):
    if not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", reference):
        raise ValueError("image pool requires an immutable source")
    dockerfile = "FROM " + reference + "\n"
    if preparation == "swesmith-v1":
        dockerfile += ("USER root\nWORKDIR /testbed\n"
                       "RUN git fetch origin '+refs/heads/*:refs/remotes/origin/*'\n"
                       "RUN command -v rg || (apt-get update && apt-get install -y --no-install-recommends ripgrep)\n")
    elif preparation != "source":
        raise ValueError("unsupported pool preparation")
    return dockerfile


def rebuild_generation(plan):
    generation = plan.get("rebuild_generation", "")
    if not isinstance(generation, str) or (generation and not re.fullmatch(r"[a-z0-9-]{1,32}", generation)):
        raise ValueError("invalid rebuild generation")
    return generation


def image_identity(reference, preparation="source", generation=""):
    inputs = {"schema": 1, "platform": "linux/amd64", "dockerfile": image_recipe(reference, preparation)}
    if generation:
        inputs["rebuild_generation"] = generation
    return hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()


def receipt_image(receipt, image_id):
    build = receipt.get("build", {})
    candidates = [receipt.get("published") or {}]
    if build.get("status") == "succeeded":
        candidates.append(build.get("image") or {})
    for image in candidates:
        if image.get("id") == image_id and image.get("pushed") and image.get("manifest_digest"):
            return image
    return None


def recorded_build_failed(receipt, previous):
    if receipt.get("build", {}).get("status") == "failed":
        return True
    return bool(previous and previous.get("status") == "failed"
                and "image build not found" in previous.get("error", ""))


def catalog_publication(item, key):
    """Recover a registry pointer; callers must verify its signed closure first."""
    from ucloud_sandboxes.images import ImageRecord
    from ucloud_sandboxes.models import utc_now
    if not item or item.get("status") != "ready" or item.get("key") != key:
        return None
    if item.get("image_id") != "precomputed-" + key[:32]:
        raise ValueError("catalog preparation identity mismatch")
    reference = item["reference"]
    if not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", reference):
        raise ValueError("catalog reference is not pinned")
    tag, digest = reference.split("@")
    now = utc_now()
    return ImageRecord(id=item["image_id"], tag=tag, source="registry", state="available",
                       created_at=now, updated_at=now, pushed=True, manifest_digest=digest).to_dict()


def admission(used, available, initial_used, reserved, *, growth_limit, free_floor, estimate):
    if available - reserved - estimate < free_floor:
        return "free-space reserve"
    if max(0, used - initial_used) + reserved + estimate > growth_limit:
        return "batch storage budget"
    return None


def register_import_alias(store, record, alias):
    """Make normal external-image creates reuse the prepared artifact.

    Publication is insert-only under the existing image-store transaction. An
    existing import is never replaced by a preparation snapshot of a mutable tag.
    """
    from ucloud_sandboxes.image_import import import_image_id
    target = replace(record, id=import_image_id(alias), source="registry")
    with store._transaction(write=True) as connection:
        previous = connection.execute(
            "SELECT record_json FROM image_state_v1_images WHERE record_id = ?", (target.id,)
        ).fetchone()
        if previous is not None:
            old = json.loads(previous[0])
            return "already-ready" if (old.get("tag"), old.get("manifest_digest")) == (target.tag, target.manifest_digest) else "preserved-existing"
        store._put(connection, target)
        return "registered"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--sdk-wheel", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path("/etc/ucloud-sandboxes/deployment.json"))
    parser.add_argument("--gateway", required=True)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--growth-limit-gib", type=int, default=128)
    parser.add_argument("--free-floor-gib", type=int, default=200)
    parser.add_argument("--max-image-gib", type=int, default=5)
    parser.add_argument("--stage-upstream", action="store_true",
                        help="copy verified OCI inputs into the managed registry before building")
    parser.add_argument("--retry-recorded-failures", action="store_true",
                        help="submit a fresh job for a recorded failed or missing build; preserve old receipts")
    args = parser.parse_args()
    if min(args.limit, args.growth_limit_gib, args.free_floor_gib, args.max_image_gib) <= 0:
        parser.error("limits must be positive")
    if not 1 <= args.workers <= 32:
        parser.error("workers must be 1..32")
    sys.path.insert(0, str(args.sdk_wheel))
    import ucloud_sandboxes_sdk as sdk
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.control_plane import _persist_registry_image_protection
    from ucloud_sandboxes.environment_artifact import load_image_environment
    from ucloud_sandboxes.environment_config import environment_registry_from_deployment
    from ucloud_sandboxes.environment_dependencies import EnvironmentDependencyResolver
    from ucloud_sandboxes.host_locks import HOST_LOCKS
    from ucloud_sandboxes.images import ImageRecord, ImageStore
    from ucloud_sandboxes.managed_registry import RegistryUsageStore, registry_repository_tag_from_image_ref
    from ucloud_sandboxes.registry_disk import registry_disk_usage

    config = DeploymentConfig.from_dict(json.loads(args.config.read_text()))
    HOST_LOCKS.configure(config.control_state_file().parent / "gateway-locks")
    token = config.sandbox_api_token_file().read_text().strip()
    registry = environment_registry_from_deployment(config)
    usage = RegistryUsageStore(config.registry_usage_file())
    dependencies = EnvironmentDependencyResolver(registry)
    image_store = ImageStore(config.image_file())
    claim_root = config.control_state_file().parent / "image-pool-locks"
    claim_root.mkdir(parents=True, exist_ok=True)
    resolve = SourceResolver(claim_root)
    plan = json.loads((args.root / "plan.json").read_text())
    if plan.get("schema") != 1:
        raise ValueError("unsupported pool plan")
    generation = rebuild_generation(plan)
    lock = (args.root / "prepare.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    catalog_path = args.root / "catalog.json"
    catalog = recover_catalog(args.root)
    if catalog_path.exists() and catalog.get("rebuild_generation", "") != generation:
        raise ValueError("use a fresh output directory for a different rebuild generation")
    catalog["rebuild_generation"] = generation
    blob_sources = None
    if args.stage_upstream:
        from stage_source_image import BlobSourceIndex
        blob_sources = BlobSourceIndex(claim_root / "upstream-blob-sources.sqlite")
        for source, previous in catalog["images"].items():
            if previous.get("status") != "ready":
                continue
            saved_path = args.root / (hashlib.sha256(source.encode()).hexdigest() + ".json")
            if saved_path.exists():
                saved = json.loads(saved_path.read_text())
                repository, _ = registry_repository_tag_from_image_ref(previous["reference"])
                blob_sources.remember(saved.get("resolved", {}), repository)
    disk = registry_disk_usage(config)
    if disk is None:
        raise ValueError("pool preparation requires measurable registry storage")
    # Persist the batch baseline: restarting must not reset its storage allowance.
    if "initial_used_bytes" not in catalog:
        catalog["initial_used_bytes"] = disk.used_bytes
        save(catalog_path, catalog)
    guard = threading.Lock()
    inventory_guard = threading.Lock()
    inventory = None
    reserved = 0
    last_checkpoint = time.monotonic()
    since_checkpoint = 0

    def fleet_image(client, image_id):
        nonlocal inventory
        # Fleet inventory can be large and includes remote node queries. Fetch
        # it once; new pool publications are persisted in the gateway store.
        with inventory_guard:
            if inventory is None:
                inventory = {r["id"]: r for r in client.list_images() if r.get("manifest_digest")}
            return inventory.get(image_id)

    def prepare(item):
        nonlocal reserved, last_checkpoint, since_checkpoint
        source = item["source"]
        source_key = hashlib.sha256(source.encode()).hexdigest()
        receipt_path = args.root / (source_key + ".json")
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else {"source": source}
        reservation = 0
        client = sdk.SandboxClient(args.gateway, api_token=token, timeout_seconds=120)
        try:
            if receipt["source"] != source:
                raise ValueError("receipt source mismatch")
            if "resolved" not in receipt:
                # Once even the minimum reservation cannot fit, avoid spending
                # public-registry requests on thousands of inadmissible inputs.
                with guard:
                    current = registry_disk_usage(config)
                    reason = admission(current.used_bytes, current.available_bytes,
                                       catalog["initial_used_bytes"], reserved,
                                       growth_limit=args.growth_limit_gib * GIB,
                                       free_floor=args.free_floor_gib * GIB, estimate=GIB)
                    if reason:
                        raise RuntimeError("deferred: " + reason)
                # A recovery plan keeps known upstream digests, while resolving
                # unvisited mutable tags is explicitly a new upstream snapshot.
                receipt["resolved"] = resolve(item.get("pinned_source", source))
                save(receipt_path, receipt)
            resolved = receipt["resolved"]
            preparation = item.get("preparation", "source")
            if preparation == "source" and resolved.get("onbuild"):
                raise RuntimeError("deferred: inherited ONBUILD triggers require a direct source import")
            key = image_identity(resolved["reference"], preparation, generation)
            image_id = "precomputed-" + key[:32]
            # Serialize aliases for the same immutable image, across local runs.
            with (claim_root / (key + ".lock")).open("a") as image_lock:
                fcntl.flock(image_lock, fcntl.LOCK_EX)
                local = image_store.get(image_id)
                published = local.to_dict() if local and local.pushed and local.manifest_digest else None
                if published is None:
                    # Successful receipts survive builder history. The signed
                    # registry closure is still checked below before reuse.
                    published = receipt_image(receipt, image_id)
                if published is None:
                    with guard:
                        previous = catalog["images"].get(source)
                    published = catalog_publication(previous, key)
                if published is None:
                    published = fleet_image(client, image_id)
                if published is None:
                    if resolved["compressed_bytes"] > args.max_image_gib * GIB:
                        raise RuntimeError("deferred: source exceeds per-image compressed size limit")
                    with guard:
                        current = registry_disk_usage(config)
                        estimate = max(GIB, resolved["compressed_bytes"] * 4)
                        reason = admission(current.used_bytes, current.available_bytes,
                                           catalog["initial_used_bytes"], reserved,
                                           growth_limit=args.growth_limit_gib * GIB,
                                           free_floor=args.free_floor_gib * GIB, estimate=estimate)
                        if reason:
                            raise RuntimeError("deferred: " + reason)
                        reservation = estimate
                        reserved += reservation
                    context = args.root / image_id
                    context.mkdir(exist_ok=True)
                    build_source = resolved["reference"]
                    if args.stage_upstream:
                        from stage_source_image import stage_source
                        if "manifest_json" not in resolved:
                            # Upgrade an old receipt by digest, never by its mutable tag.
                            resolved = resolve(resolved["reference"])
                            receipt["resolved"] = resolved
                            save(receipt_path, receipt)
                        staging_metrics = {}
                        build_source = stage_source(resolved, registry.client, claim_root,
                            publication_url=config.registry_worker_url, metrics=staging_metrics,
                            blob_sources=blob_sources,
                            protect=lambda ref, owner: _persist_registry_image_protection(
                                usage, ref, owner, touch=True, persistent=True))
                        receipt["staged_source"] = build_source
                        receipt["staging"] = staging_metrics
                        save(receipt_path, receipt)
                        print(json.dumps({"source": source, "status": "staged", **staging_metrics}), flush=True)
                    (context / "Dockerfile").write_text(image_recipe(build_source, preparation))
                    build_path = claim_root / (key + ".build.json")
                    previous_path = args.root / (key + ".build.json")
                    accepted_path = build_path if build_path.exists() else previous_path
                    accepted = json.loads(accepted_path.read_text()) if accepted_path.exists() else None
                    with guard:
                        previous = catalog["images"].get(source)
                    if accepted and args.retry_recorded_failures and recorded_build_failed(receipt, previous):
                        history = args.root / "attempts"
                        history.mkdir(exist_ok=True)
                        save(history / (key + "-" + hashlib.sha256(accepted["build_id"].encode()).hexdigest()[:16] + ".json"),
                             {"accepted": accepted, "receipt": receipt, "previous": previous})
                        accepted = None
                    if accepted is None:
                        accepted = client.submit_image_build(sdk.Image.from_dockerfile(name=image_id, context_path=context),
                                                             timeout_seconds=600)
                    save(build_path, accepted)
                    print(json.dumps({"source": source, "status": "building", "build_id": accepted["build_id"]}), flush=True)
                    build = client.wait_for_image_build(accepted["build_id"], timeout_seconds=7200, poll_interval_seconds=10)
                    receipt["build"] = build
                    save(receipt_path, receipt)
                    if build.get("status") != "succeeded":
                        raise RuntimeError("build failed: " + str(build.get("error", ""))[-500:])
                    published = build["image"]
                reference = published["tag"].split("@", 1)[0] + "@" + published["manifest_digest"]
                repository, _ = registry_repository_tag_from_image_ref(reference)
                environment_root, environment = load_image_environment(registry, repository, published["manifest_digest"])
                components = [registry.load(d) for d in environment.components]
                owner = "image-pool:" + key
                if not _persist_registry_image_protection(usage, reference, owner, touch=True, persistent=True,
                                                           dependency_resolver=dependencies):
                    raise RuntimeError("image closure was not retained")
                with guard:
                    previous = catalog["images"].get(source)
                if not (previous and previous.get("status") == "ready" and previous.get("reference") == reference):
                    sandbox = "pool-check-" + uuid4().hex[:16]
                    try:
                        client.create_sandbox(sdk.SandboxSpec(id=sandbox, image=sdk.Image.from_registry(reference),
                            command=["/bin/sh", "-c", "sleep 600"], cpus=1, memory_mb=2048, disk_mb=1024, ttl_seconds=600,
                            security=sdk.SandboxSecuritySpec(user="0:0")),
                            request_timeout_seconds=600)
                        probe = "test -d / && test -r /etc/os-release"
                        if preparation == "swesmith-v1":
                            probe += (" && command -v rg && git -C /testbed rev-parse HEAD"
                                      " && test $(git -C /testbed for-each-ref refs/remotes/origin | wc -l) -gt 0")
                        checked = client.exec(sandbox, ["/bin/sh", "-c", probe], timeout_seconds=60)
                        receipt["validation"] = {"user": "0:0", "exit_code": checked.exit_code,
                                                 "stdout": checked.stdout[-1000:], "stderr": checked.stderr[-1000:]}
                        save(receipt_path, receipt)
                        if checked.exit_code != 0:
                            raise RuntimeError("source image smoke failed: " + checked.stderr[-500:])
                    finally:
                        try:
                            client.delete_sandbox(sandbox)
                        except sdk.SandboxApiError as exc:
                            if exc.status_code != 404:
                                raise
                result = {**item, "status": "ready", "key": key, "reference": reference,
                          "source_reference": resolved["reference"], "environment_root": environment_root,
                          "image_id": image_id, "retention_owner": owner,
                          "validation": "signed artifact closure and sandbox mount/exec; SWE-smith also checks ripgrep and fetched refs; task-specific setup remains",
                          "components": [{"digest": c.image_digest, "bytes": c.image_size} for c in components]}
                record = ImageRecord.from_dict({k: v for k, v in published.items()
                                                if k in ImageRecord.__dataclass_fields__})
                image_store.upsert_if_changed(record)
                receipt["published"] = record.to_dict()
                save(receipt_path, receipt)
                if blob_sources is not None:
                    blob_sources.remember(resolved, repository)
                host, repository, selector = registry_parts(source)
                separator = "@" if selector.startswith("sha256:") else ":"
                # A preparation recipe changes image contents/defaults. Only
                # faithful source imports may occupy the upstream import key.
                aliases = ({source, resolved["reference"], host + "/" + repository + separator + selector}
                           if preparation == "source" else set())
                if host == "docker.io" and preparation == "source":
                    aliases.add(repository + separator + selector)
                result["import_aliases"] = {alias: register_import_alias(image_store, record, alias)
                                            for alias in sorted(aliases)}
        except Exception as error:
            result = {**item, "status": "deferred" if str(error).startswith("deferred:") else "failed",
                      "error": str(error)[-1000:]}
        finally:
            with guard:
                reserved -= reservation
        # Journal each result before periodically replacing the large snapshot.
        # This bounds write amplification for pools with tens of thousands of
        # images, without losing acknowledged results after interruption.
        journal_result(args.root, result)
        with guard:
            catalog["images"][source] = result
            catalog["updated_at_unix"] = time.time()
            since_checkpoint += 1
            if since_checkpoint >= 50 or time.monotonic() - last_checkpoint >= 30:
                save(catalog_path, catalog)
                since_checkpoint = 0
                last_checkpoint = time.monotonic()
        print(json.dumps({"source": source, "status": result["status"], "error": result.get("error")}), flush=True)
        return result

    # Source aliases are grouped before execution; identical resolved digests are
    # serialized by the per-image lock and rechecked against published artifacts.
    unique = {}
    for item in plan["images"]:
        if item["source"] in unique:
            raise ValueError("duplicate source in pool plan")
        unique[item["source"]] = item
    with ThreadPoolExecutor(max_workers=args.workers) as workers:
        results = list(workers.map(prepare, list(unique.values())[:args.limit]))
    save(catalog_path, catalog)
    counts = {status: sum(r["status"] == status for r in results) for status in ("ready", "failed", "deferred")}
    print(json.dumps(counts), flush=True)
    if counts["failed"] or counts["deferred"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
