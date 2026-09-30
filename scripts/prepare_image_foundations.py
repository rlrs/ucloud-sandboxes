#!/usr/bin/env python3
"""Build and retain a bounded foundation plan on the gateway; resume by artifact.

Credentials remain on the gateway. Contexts and plan.json are produced by the
offline planner; --limit bounds the number of distinct foundations processed.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
from pathlib import Path
import re
import sys
import threading
import time
from uuid import uuid4


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def validate_context(root, item):
    key = item["key"]
    if len(key) != 64 or any(c not in "0123456789abcdef" for c in key):
        raise ValueError("invalid foundation identity")
    family = item.get("family", "tmax")
    if family not in {"tmax", "tmax-inline", "openswe", "terminal-prefix"} or item["image_id"] != "foundation-" + family + "-" + key[:32]:
        raise ValueError("foundation image identity mismatch")
    context = root / item["image_id"]
    files = {"Dockerfile", "base_install.sh"} if family in {"tmax", "tmax-inline"} else {"Dockerfile"}
    if {p.name for p in context.iterdir()} != files:
        raise ValueError("foundation context must contain only its dependency inputs")
    inputs = {"schema": 1, "platform": "linux/amd64", "dockerfile": (context / "Dockerfile").read_text()}
    if family in {"tmax", "tmax-inline"}:
        inputs["base_install_sha256"] = hashlib.sha256((context / "base_install.sh").read_bytes()).hexdigest()
    if hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest() != key:
        raise ValueError("foundation context changed after planning")
    return context


def build_image_id(item, generation):
    if not generation:
        return item["image_id"]
    digest = hashlib.sha256(json.dumps([item["key"], generation]).encode()).hexdigest()
    return "foundation-" + item.get("family", "tmax") + "-" + digest[:32]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--sdk-wheel", required=True, type=Path)
    parser.add_argument("--config", type=Path, default=Path("/etc/ucloud-sandboxes/deployment.json"))
    parser.add_argument("--gateway", required=True)
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--growth-limit-gib", type=int, default=128)
    parser.add_argument("--free-floor-gib", type=int, default=300)
    parser.add_argument("--reservation-gib", type=int, default=8)
    args = parser.parse_args()
    if min(args.limit, args.growth_limit_gib, args.free_floor_gib, args.reservation_gib) < 1:
        parser.error("limits must be positive")
    if not 1 <= args.workers <= 32:
        parser.error("workers must be 1..32")
    from prepare_image_pool import admission, GIB, rebuild_generation
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
    from ucloud_sandboxes.models import utc_now
    from ucloud_sandboxes.registry_disk import registry_disk_usage

    config = DeploymentConfig.from_dict(json.loads(args.config.read_text()))
    HOST_LOCKS.configure(config.control_state_file().parent / "gateway-locks")
    token = config.sandbox_api_token_file().read_text().strip()
    registry = environment_registry_from_deployment(config)
    usage = RegistryUsageStore(config.registry_usage_file())
    dependencies = EnvironmentDependencyResolver(registry)
    image_store = ImageStore(config.image_file())
    claim_root = config.control_state_file().parent / "image-foundation-locks"
    claim_root.mkdir(parents=True, exist_ok=True)
    plan = json.loads((args.root / "plan.json").read_text())
    if plan.get("schema") != 1:
        raise ValueError("unsupported foundation plan")
    generation = rebuild_generation(plan)
    identities = [item["image_id"] for item in plan["foundations"]]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate foundation identity in plan")
    with (args.root / "prepare.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        catalog_path = args.root / "catalog.json"
        catalog = json.loads(catalog_path.read_text()) if catalog_path.exists() else {"schema": 1, "foundations": {}}
        if catalog_path.exists() and catalog.get("rebuild_generation", "") != generation:
            raise ValueError("use a fresh output directory for a different rebuild generation")
        catalog["rebuild_generation"] = generation
        outcomes = args.root / "results"
        outcomes.mkdir(exist_ok=True)
        for path in outcomes.glob("*.json"):
            item = json.loads(path.read_text())
            if path.stem != item["key"]:
                raise ValueError("foundation result identity mismatch")
            if item.get("validated") is True:
                catalog["foundations"][item["key"]] = item
                catalog.setdefault("failures", {}).pop(item["key"], None)
            else:
                catalog["foundations"].pop(item["key"], None)
                catalog.setdefault("failures", {})[item["key"]] = item
        disk = registry_disk_usage(config)
        if disk is None:
            raise ValueError("foundation preparation requires measurable registry storage")
        catalog.setdefault("initial_used_bytes", disk.used_bytes)
        save(catalog_path, catalog)
        catalog_guard = threading.Lock()
        inventory_guard = threading.Lock()
        inventory = None
        reserved = 0
        last_checkpoint = time.monotonic()
        since_checkpoint = 0

        def fleet_image(client, image_id):
            nonlocal inventory
            with inventory_guard:
                if inventory is None:
                    inventory = {r["id"]: r for r in client.list_images() if r.get("manifest_digest")}
                return inventory.get(image_id)

        def prepare(item, reservation):
            nonlocal reserved
            client = sdk.SandboxClient(args.gateway, api_token=token, timeout_seconds=120)
            context = validate_context(args.root, item)
            image_id = build_image_id(item, generation)
            receipt_path = args.root / (image_id + ".receipt.json")
            receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else {"key": item["key"]}
            if receipt["key"] != item["key"]:
                raise ValueError("receipt identity mismatch")
            # Published artifacts outlive builder VMs and their build records.
            local = image_store.get(image_id)
            published = local.to_dict() if local and local.pushed and local.manifest_digest else None
            completed = receipt.get("build", {})
            saved = completed.get("image") or {}
            if (published is None and completed.get("status") == "succeeded" and saved.get("id") == image_id
                    and saved.get("pushed") and saved.get("manifest_digest")):
                published = saved
            with catalog_guard:
                previous = catalog["foundations"].get(item["key"])
            if published is None and previous and previous.get("validated") is True:
                if previous.get("key") != item["key"] or previous.get("image_id") != image_id:
                    raise ValueError("foundation catalog identity mismatch")
                if not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", previous["reference"]):
                    raise ValueError("foundation catalog reference is not pinned")
                tag, digest = previous["reference"].split("@")
                now = utc_now()
                published = ImageRecord(id=image_id, tag=tag, source="registry", state="available",
                    created_at=now, updated_at=now, pushed=True, manifest_digest=digest).to_dict()
            if published is None:
                published = fleet_image(client, image_id)
            if published is None:
                with catalog_guard:
                    current = registry_disk_usage(config)
                    estimate = args.reservation_gib * GIB
                    reason = admission(current.used_bytes, current.available_bytes,
                                       catalog["initial_used_bytes"], reserved,
                                       growth_limit=args.growth_limit_gib * GIB,
                                       free_floor=args.free_floor_gib * GIB, estimate=estimate)
                    if reason:
                        raise RuntimeError("deferred: " + reason)
                    reserved += estimate
                    reservation[0] = estimate
                accepted_path = claim_root / (image_id + ".build.json")
                if not receipt.get("build_id") and accepted_path.exists():
                    receipt.update(json.loads(accepted_path.read_text()))
                if not receipt.get("build_id"):
                    build = client.submit_image_build(sdk.Image.from_dockerfile(name=image_id, context_path=context),
                                                      timeout_seconds=600)
                    receipt.update(build_id=build["build_id"], build=build)
                    save(accepted_path, {"build_id": build["build_id"]})
                    save(receipt_path, receipt)
                print(json.dumps({"image": image_id, "build_id": receipt["build_id"], "status": "waiting"}), flush=True)
                build = client.wait_for_image_build(receipt["build_id"], timeout_seconds=7200, poll_interval_seconds=10)
                receipt["build"] = build
                save(receipt_path, receipt)
                if build.get("status") != "succeeded":
                    raise RuntimeError("foundation build failed: " + str(build.get("error", ""))[-1000:])
                published = build["image"]
            digest = published["manifest_digest"]
            reference = published["tag"].split("@", 1)[0] + "@" + digest
            repository, _ = registry_repository_tag_from_image_ref(reference)
            environment_root, environment = load_image_environment(registry, repository, digest)
            components = [registry.load(d) for d in environment.components]
            owner = "image-foundation:" + item["key"]
            if not _persist_registry_image_protection(usage, reference, owner, touch=True, persistent=True,
                                                       dependency_resolver=dependencies):
                raise RuntimeError("foundation artifact closure was not retained")
            record = ImageRecord.from_dict({k: v for k, v in published.items() if k in ImageRecord.__dataclass_fields__})
            if previous and previous.get("validated") is True and previous.get("reference") == reference:
                image_store.upsert_if_changed(record)
                print(json.dumps({"image": image_id, "status": "ready", "artifact_reused": True}), flush=True)
                return previous
            sandbox = "foundation-check-" + uuid4().hex[:16]
            started = time.monotonic()
            try:
                client.create_sandbox(sdk.SandboxSpec(id=sandbox, image=sdk.Image.from_registry(reference),
                    command=["sleep", "600"], cpus=1, memory_mb=2048, disk_mb=1024, ttl_seconds=600),
                    request_timeout_seconds=600)
                if item.get("family", "tmax") == "tmax":
                    command = ["python3", "-c",
                        "import json,numpy,scipy,sklearn,pandas,torch,torchvision,pytest; "
                        "assert torch.version.cuda is None; "
                        "print(json.dumps({'numpy':numpy.__version__,'torch':torch.__version__,'cpu_only':True}))"]
                elif item.get("family") == "tmax-inline":
                    command = ["/bin/sh", "-c", 'dpkg-query -W >/dev/null && printf \'{"packages_readable":true}\\n\'']
                elif item.get("family") == "terminal-prefix":
                    command = ["/bin/sh", "-c", 'test -d / && test -r /etc/os-release && printf \'{"filesystem_readable":true}\\n\'']
                else:
                    command = ["/opt/conda/envs/testbed/bin/python", "-c",
                        "import json,ssl,sys; assert sys.version_info[:2]==tuple(map(int,sys.argv[1].split('.'))); "
                        "print(json.dumps({'python':sys.version,'openssl':ssl.OPENSSL_VERSION}))", item["python_version"]]
                checked = client.exec(sandbox, command, timeout_seconds=120)
                if checked.exit_code != 0:
                    raise RuntimeError("foundation smoke failed: " + checked.stderr[-1000:])
                validation = json.loads(checked.stdout)
            finally:
                try:
                    client.delete_sandbox(sandbox)
                except sdk.SandboxApiError as exc:
                    if exc.status_code != 404:
                        raise
            ready = {**item, "image_id": image_id, "reference": reference, "environment_root": environment_root,
                     "components": [{"digest": c.image_digest, "bytes": c.image_size} for c in components],
                     "retention_owner": owner, "validated": True, "validation": validation,
                     "smoke_seconds": time.monotonic() - started}
            image_store.upsert_if_changed(record)
            print(json.dumps({"image": image_id, "status": "ready", "tasks": item["tasks"],
                              "erofs_bytes": sum(c.image_size for c in components)}), flush=True)
            return ready

        def bounded_prepare(item):
            nonlocal reserved, last_checkpoint, since_checkpoint
            reservation = [0]
            # Validate identity before using it as a shared lock pathname.
            try:
                validate_context(args.root, item)
                with (claim_root / (item["image_id"] + ".lock")).open("a") as claim:
                    fcntl.flock(claim, fcntl.LOCK_EX)
                    result = prepare(item, reservation)
            except Exception as error:
                result = {"key": item["key"], "image_id": item["image_id"], "validated": False,
                          "status": "deferred" if str(error).startswith("deferred:") else "failed",
                          "error": str(error)[-1000:]}
                print(json.dumps(result), flush=True)
            finally:
                with catalog_guard:
                    reserved -= reservation[0]
            # Invalid identities fail before any path is derived from their key.
            key = result["key"]
            if not re.fullmatch(r"[a-f0-9]{64}", key):
                raise ValueError("invalid foundation identity")
            save(outcomes / (key + ".json"), result)
            with catalog_guard:
                if result.get("validated") is True:
                    catalog["foundations"][key] = result
                    catalog.setdefault("failures", {}).pop(key, None)
                else:
                    catalog["foundations"].pop(key, None)
                    catalog.setdefault("failures", {})[key] = result
                since_checkpoint += 1
                if since_checkpoint >= 50 or time.monotonic() - last_checkpoint >= 30:
                    save(catalog_path, catalog)
                    last_checkpoint = time.monotonic()
                    since_checkpoint = 0
            return result.get("validated") is True

        with ThreadPoolExecutor(max_workers=args.workers) as workers:
            results = list(workers.map(bounded_prepare, plan["foundations"][:args.limit]))
        save(catalog_path, catalog)
        print(json.dumps({"ready": sum(results), "failed_or_deferred": len(results) - sum(results)}), flush=True)
        if not all(results):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
