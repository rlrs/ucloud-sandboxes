#!/usr/bin/env python3
"""Exercise retained images through faithful source aliases or prepared IDs."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import time
from uuid import uuid4


def matching_alias(alias, reference):
    from ucloud_sandboxes.managed_registry import canonical_image_digest_ref
    return alias is not None and alias.pushed and alias.digest_ref == canonical_image_digest_ref(reference)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sdk-wheel", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path("/etc/ucloud-sandboxes/deployment.json"))
    parser.add_argument("--gateway", required=True)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    if not 1 <= args.workers <= 16 or not 1 <= args.limit <= 1000:
        parser.error("workers must be 1..16; limit must be 1..1000")
    if args.output.exists():
        parser.error("use a new output path")
    sys.path.insert(0, str(args.sdk_wheel))
    import ucloud_sandboxes_sdk as sdk
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.image_import import import_image_id
    from ucloud_sandboxes.images import ImageStore

    config = DeploymentConfig.from_dict(json.loads(args.config.read_text()))
    token = config.sandbox_api_token_file().read_text().strip()
    store = ImageStore(config.image_file())
    candidates = {}
    for path in args.catalog:
        for source, item in json.loads(path.read_text())["images"].items():
            if item.get("status") != "ready":
                continue
            plain = item.get("preparation", "source") == "source"
            if plain and matching_alias(store.get(import_image_id(source)), item["reference"]):
                lookup = "original_source"
            elif matching_alias(store.get(item["image_id"]), item["reference"]):
                lookup = "prepared_image_id"
            else:
                lookup = "unavailable"
            candidates[source] = {**item, "lookup": lookup}
    if not candidates:
        raise RuntimeError("no ready catalog images")

    def check(pair):
        source, item = pair
        client = sdk.SandboxClient(args.gateway, api_token=token, timeout_seconds=120)
        sandbox = "pool-qualification-" + uuid4().hex[:16]
        result = {"source": source, "families": item["families"], "status": "failed"}
        plain = item["lookup"] == "original_source"
        result["lookup"] = item["lookup"]
        if item["lookup"] == "unavailable":
            return {**result, "error": "ready catalog image has no matching gateway record"}
        try:
            # A pre-existing import build would make the no-new-build assertion
            # ambiguous; record its identity and require it to remain unchanged.
            def import_build_id():
                try:
                    return client.get_image_build(import_image_id(source), timeout_seconds=30)["build_id"]
                except sdk.SandboxApiError as exc:
                    if exc.status_code == 404:
                        return None
                    raise

            before = import_build_id()
            start = time.monotonic()
            image = sdk.Image.from_registry(source) if plain else sdk.Image.from_name(item["image_id"])
            client.create_sandbox(sdk.SandboxSpec(id=sandbox, image=image,
                command=["/bin/sh", "-c", "sleep 180"], cpus=0.25, memory_mb=512, disk_mb=512,
                ttl_seconds=180, security=sdk.SandboxSecuritySpec(user="0:0")), request_timeout_seconds=120)
            result["create_seconds"] = time.monotonic() - start
            start = time.monotonic()
            executed = client.exec(sandbox, ["/bin/sh", "-c",
                "printf prepared-image-check > /tmp/pool-check && test $(cat /tmp/pool-check) = prepared-image-check"],
                timeout_seconds=30)
            result["exec_seconds"] = time.monotonic() - start
            if executed.exit_code != 0:
                raise RuntimeError("image exec/file check failed: " + executed.stderr[-500:])
            after = import_build_id()
            if after != before:
                raise RuntimeError("an original-source request submitted a new import build")
            result.update(status="passed", new_import_build=False)
        except Exception as error:
            result["error"] = str(error)[-1000:]
        finally:
            try:
                client.delete_sandbox(sandbox)
            except Exception as exc:
                if not isinstance(exc, sdk.SandboxApiError) or exc.status_code != 404:
                    result.update(status="failed", cleanup_error=str(exc)[-500:])
        print(json.dumps(result), flush=True)
        return result

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as workers:
        results = list(workers.map(check, list(candidates.items())[:args.limit]))
    times = sorted(r["create_seconds"] for r in results if r["status"] == "passed")
    def percentile(fraction):
        return times[min(len(times) - 1, int(fraction * len(times)))] if times else None
    report = {"schema": 1, "images": len(results), "workers": args.workers,
              "passed": sum(r["status"] == "passed" for r in results),
              "elapsed_seconds": time.monotonic() - start,
              "create_p50_seconds": percentile(.5), "create_p95_seconds": percentile(.95),
              "create_max_seconds": times[-1] if times else None, "results": results}
    with args.output.open("x") as output:
        output.write(json.dumps(report, indent=2) + "\n")
    if report["passed"] != report["images"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
