#!/usr/bin/env python3
"""Test owned build cancellation on an explicitly supplied disposable Linux builder.

Uses the installed runtime and existing shared BuildKit driver. No SSH, provider
calls, daemon signalling, global prune or GC. Only UUID-owned manifests are
deleted; small local BuildKit cache entries follow its normal GC policy.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
from urllib.parse import urlsplit
from uuid import uuid4


MARKER_KEY = "UCLOUD_BUILD_DEADLINE_WITNESS"


def validate_args(args):
    endpoint = urlsplit(args.registry_url)
    if (endpoint.scheme not in {"http", "https"} or not endpoint.netloc
            or endpoint.path not in {"", "/"} or endpoint.username or endpoint.password
            or endpoint.query or endpoint.fragment or endpoint.netloc != args.registry_authority):
        raise ValueError("registry URL and push authority must match without credentials or paths")
    if not re.fullmatch(r"[A-Za-z0-9./:_-]+@sha256:[0-9a-f]{64}", args.base_image):
        raise ValueError("base image must be pinned by digest and contain python3 and env")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", args.builder):
        raise ValueError("explicit existing builder name is required")
    if not re.fullmatch(r"[0-9]+", args.owned_builder_id):
        raise ValueError("declare the disposable builder's numeric provider ID")
    if not 1 <= args.execution_timeout_seconds <= 10:
        raise ValueError("timeout canary execution budget must be 1..10 seconds")
    if not 30 <= args.normal_timeout_seconds <= 300:
        raise ValueError("normal build budget must be 30..300 seconds")


def marked_processes(marker, *, proc_root=Path("/proc")):
    """Read bounded environments and return only exact owned-marker PIDs/counts."""
    expected = (MARKER_KEY + "=" + marker).encode()
    found, unreadable = [], 0
    processes = [entry for entry in proc_root.iterdir() if entry.name.isdecimal()]
    if len(processes) > 32768:
        raise RuntimeError("process inventory exceeds canary bound")
    for entry in processes:
        try:
            with (entry / "environ").open("rb") as source:
                value = source.read(1024 * 1024 + 1)
            if len(value) > 1024 * 1024:
                unreadable += 1
            elif expected in value.split(b"\0"):
                found.append(int(entry.name))
        except (FileNotFoundError, ProcessLookupError):
            pass  # Ordinary process exit during a snapshot.
        except PermissionError:
            unreadable += 1
    return {"pids": sorted(found), "unreadable_environments": unreadable}


def run(args):
    from ucloud_sandboxes.build_deadline import build_execution_deadline
    import ucloud_sandboxes.build_deadline as deadline_module
    import ucloud_sandboxes.images as image_module
    from ucloud_sandboxes.images import (
        DockerImageRuntime, ImageBuildSpec, ImageBuildStore, ImageManager,
        ImageStore, MaterializedBuildContext,
    )
    from ucloud_sandboxes.managed_registry import (
        RegistryClient, RegistryRequestError, normalize_manifest_digest,
    )

    validate_args(args)
    if os.name != "posix" or os.geteuid() != 0 or not Path("/proc/self/environ").is_file():
        raise ValueError("run as root on the explicitly owned disposable Linux builder")
    root = args.work_root.absolute()
    root.mkdir(mode=0o700, parents=False)  # Never reuse earlier evidence/state.
    marker = uuid4().hex
    repository = "ucloud-managed/deadline-proof-" + marker
    tags = ("warm", "timeout", "after")
    client = RegistryClient(args.registry_url, timeout_seconds=3)
    runtime = DockerImageRuntime(
        docker_binary=args.docker, buildx_direct_push=True, buildx_builder=args.builder,
    )
    manager = ImageManager(ImageStore(root / "images.sqlite"), runtime,
                           max_active_builds=1, max_queued_builds=0,
                           build_execution_timeout_seconds=args.normal_timeout_seconds)
    receipt = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "operator_supplied_owned_builder_id": args.owned_builder_id,
        "builder": args.builder, "registry_repository": repository,
        "base_image": args.base_image, "execution_timeout_seconds": args.execution_timeout_seconds,
        "normal_timeout_seconds": args.normal_timeout_seconds, "witness_sleep_seconds": 60,
        "source_hashes": {Path(module.__file__).name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
                          for module in (image_module, deadline_module)},
        "builds": [], "witness_pids_seen": [], "max_unreadable_environments": 0,
        "proof_passed": False, "cleanup": {"manifest_digests_deleted": [], "errors": []},
        "limitations": ["This tests build/push cancellation and slot recovery, not EROFS publication.",
                       "Process evidence proves the marked owned RUN stopped; it is not a whole-daemon leak audit.",
                       "Local BuildKit cache remains under normal GC; no shared daemon or unrelated process is signalled."],
    }

    def save():
        temporary = root / "receipt.tmp"
        temporary.write_text(json.dumps(receipt, indent=2, allow_nan=False) + "\n")
        temporary.chmod(0o600)
        temporary.replace(root / "receipt.json")

    def witness():
        seen = marked_processes(marker)
        receipt["max_unreadable_environments"] = max(
            receipt["max_unreadable_environments"], seen["unreadable_environments"])
        receipt["witness_pids_seen"] = sorted(set(receipt["witness_pids_seen"]) | set(seen["pids"]))
        return seen["pids"]

    def owned_tags():
        try:
            return client.tags(repository)
        except RegistryRequestError as exc:
            if exc.status_code == 404:
                return []
            raise

    def build(label, *, timeout):
        manager.build_execution_timeout_seconds = timeout
        instruction = (["env", MARKER_KEY + "=" + marker, "python3", "-c", "import time; time.sleep(60)"]
                       if label == "timeout" else ["python3", "-c", "pass"])
        dockerfile = ("FROM " + args.base_image + "\nRUN " + json.dumps(instruction) + "\n").encode()
        identity = "archive:sha256:" + hashlib.sha256(dockerfile).hexdigest()

        def materialize():
            directory = tempfile.TemporaryDirectory(prefix=label + "-", dir=root)
            context = Path(directory.name)
            (context / "Dockerfile").write_bytes(dockerfile)
            return MaterializedBuildContext(context, directory, identity)

        started = time.monotonic()
        record, accepted = manager.start_build(
            ImageBuildSpec(id="deadline-" + marker + "-" + label,
                           tag=args.registry_authority + "/" + repository + ":" + label,
                           context_path="."),
            context_identity=identity, materialize_context=materialize, push=True,
        )
        if not accepted:
            raise RuntimeError("new canary build unexpectedly joined existing work")
        row = {"label": label, "build_id": record.build_id, "image_id": record.image_id}
        receipt["builds"].append(row)
        save()
        guard = time.monotonic() + timeout + 20
        while time.monotonic() < guard:
            if label == "timeout":
                witness()
            record = manager.wait_for_build(record.build_id, timeout_seconds=0.05)
            if record.terminal and manager.active_build_count() == 0:
                break
        else:
            raise RuntimeError("canary manager did not release its execution slot")
        row.update(status=record.status, elapsed_seconds=time.monotonic() - started,
                   active_slots_after=manager.active_build_count(),
                   server_deadline_reported="server execution deadline" in record.error,
                   persisted_terminal=ImageBuildStore(root / "images.sqlite").get(record.build_id).terminal,
                   timings=record.timings)
        save()
        return row

    save()
    try:
        inspected = subprocess.run((args.docker, "buildx", "inspect", args.builder),
                                   capture_output=True, text=True, timeout=15, check=True)
        driver = next((line.split(":", 1)[1].strip() for line in inspected.stdout.splitlines()
                       if line.startswith("Driver:")), "")
        if driver != "docker-container":
            raise ValueError("canary requires the existing docker-container BuildKit driver")
        if owned_tags():
            raise ValueError("UUID-owned canary registry repository already exists")
        if witness():
            raise ValueError("fresh canary marker unexpectedly matches an existing process")
        if build("warm", timeout=args.normal_timeout_seconds)["status"] != "succeeded":
            raise RuntimeError("base warm build failed")
        result = build("timeout", timeout=args.execution_timeout_seconds)
        if result["status"] != "failed" or not result["server_deadline_reported"]:
            raise RuntimeError("sleeping build did not fail at the server deadline")
        if not receipt["witness_pids_seen"]:
            raise RuntimeError("timeout occurred before the owned RUN was observed")
        until = time.monotonic() + 10
        while witness() and time.monotonic() < until:
            time.sleep(0.1)
        receipt["witness_pids_after_timeout"] = witness()
        if receipt["witness_pids_after_timeout"] or receipt["max_unreadable_environments"]:
            raise RuntimeError("cannot prove cancellation of the owned RUN")
        if build("after", timeout=args.normal_timeout_seconds)["status"] != "succeeded":
            raise RuntimeError("normal build after deadline failed")
        receipt["witness_pids_after_followup"] = witness()
        receipt["proof_passed"] = not receipt["witness_pids_after_followup"]
    except BaseException as exc:
        receipt["error_type"] = type(exc).__name__  # Never export raw command/output/errors.
    finally:
        receipt["active_slots_at_cleanup"] = manager.active_build_count()
        if receipt["active_slots_at_cleanup"]:
            receipt["cleanup"]["errors"].append("active_owned_build_cleanup_deferred")
        else:
            with build_execution_deadline(20):
                for tag in tags:
                    try:
                        digest = normalize_manifest_digest(client.manifest_digest(repository, tag))
                        if not digest:
                            raise ValueError("owned manifest has invalid digest")
                        if digest not in receipt["cleanup"]["manifest_digests_deleted"]:
                            client.delete_manifest(repository, digest)
                            receipt["cleanup"]["manifest_digests_deleted"].append(digest)
                    except RegistryRequestError as exc:
                        if exc.status_code != 404:
                            receipt["cleanup"]["errors"].append(type(exc).__name__)
                    except Exception as exc:
                        receipt["cleanup"]["errors"].append(type(exc).__name__)
                try:
                    receipt["cleanup"]["remaining_tags"] = owned_tags()
                    if receipt["cleanup"]["remaining_tags"]:
                        receipt["cleanup"]["errors"].append("owned_tags_remain")
                except Exception as exc:
                    receipt["cleanup"]["errors"].append(type(exc).__name__)
        receipt["complete"] = bool(receipt["proof_passed"] and not receipt["cleanup"]["errors"])
        receipt["finished_at"] = datetime.now(timezone.utc).isoformat()
        save()
    print(json.dumps({"complete": receipt["complete"], "proof_passed": receipt["proof_passed"],
                      "cleanup_errors": len(receipt["cleanup"]["errors"]), "receipt": str(root / "receipt.json")}))
    return 0 if receipt["complete"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", required=True)
    parser.add_argument("--registry-authority", required=True)
    parser.add_argument("--builder", required=True)
    parser.add_argument("--owned-builder-id", required=True)
    parser.add_argument("--base-image", required=True, help="Pinned python:3.12-bookworm image or equivalent")
    parser.add_argument("--work-root", type=Path, required=True, help="New private directory; never reused")
    parser.add_argument("--execution-timeout-seconds", type=float, default=5)
    parser.add_argument("--normal-timeout-seconds", type=float, default=120)
    parser.add_argument("--docker", default="docker")
    raise SystemExit(run(parser.parse_args()))


if __name__ == "__main__":
    main()
