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
    if family not in {"tmax", "tmax-inline", "openswe"} or item["image_id"] != "foundation-" + family + "-" + key[:32]:
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--sdk-wheel", required=True, type=Path)
    parser.add_argument("--config", type=Path, default=Path("/etc/ucloud-sandboxes/deployment.json"))
    parser.add_argument("--gateway", required=True)
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--workers", type=int, choices=(1, 2), default=1)
    args = parser.parse_args()
    if not 1 <= args.limit <= 64:
        parser.error("limit must be 1..64")
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

    config = DeploymentConfig.from_dict(json.loads(args.config.read_text()))
    HOST_LOCKS.configure(config.control_state_file().parent / "gateway-locks")
    token = config.sandbox_api_token_file().read_text().strip()
    registry = environment_registry_from_deployment(config)
    usage = RegistryUsageStore(config.registry_usage_file())
    dependencies = EnvironmentDependencyResolver(registry)
    image_store = ImageStore(config.image_file())
    plan = json.loads((args.root / "plan.json").read_text())
    if plan.get("schema") != 1:
        raise ValueError("unsupported foundation plan")
    identities = [item["image_id"] for item in plan["foundations"]]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate foundation identity in plan")
    with (args.root / "prepare.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        catalog_path = args.root / "catalog.json"
        catalog = json.loads(catalog_path.read_text()) if catalog_path.exists() else {"schema": 1, "foundations": {}}
        catalog_guard = threading.Lock()

        def prepare(item):
            client = sdk.SandboxClient(args.gateway, api_token=token, timeout_seconds=120)
            context = validate_context(args.root, item)
            image_id = item["image_id"]
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
                published = next((r for r in client.list_images() if r.get("id") == image_id and r.get("manifest_digest")), None)
            if published is None:
                if not receipt.get("build_id"):
                    build = client.submit_image_build(sdk.Image.from_dockerfile(name=image_id, context_path=context),
                                                      timeout_seconds=600)
                    receipt.update(build_id=build["build_id"], build=build)
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
                return
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
            ready = {**item, "reference": reference, "environment_root": environment_root,
                     "components": [{"digest": c.image_digest, "bytes": c.image_size} for c in components],
                     "retention_owner": owner, "validated": True, "validation": validation,
                     "smoke_seconds": time.monotonic() - started}
            image_store.upsert_if_changed(record)
            with catalog_guard:
                catalog["foundations"][item["key"]] = ready
                save(catalog_path, catalog)
            print(json.dumps({"image": image_id, "status": "ready", "tasks": item["tasks"],
                              "erofs_bytes": sum(c.image_size for c in components)}), flush=True)

        with ThreadPoolExecutor(max_workers=args.workers) as workers:
            list(workers.map(prepare, plan["foundations"][:args.limit]))


if __name__ == "__main__":
    main()
