#!/usr/bin/env python3
"""Real Distribution/systemd GC qualification on an explicitly disposable host.

Install the packaged registry and GC units against an empty filesystem registry
first. This script uploads fixture images, collects garbage, injects collector
failure/SIGKILL, and checks that the registry returns with live blobs intact.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.environment_artifact import OCI_IMAGE, canonical_bytes, content_digest
from ucloud_sandboxes.managed_registry import RegistryClient
from ucloud_sandboxes.registry_sweep import DistributionTree
from ucloud_sandboxes.systemd import (
    REGISTRY_MAINTENANCE_LOCK, registry_restart_marker, REGISTRY_WRITER_LOCK,
    run_registry_sweep, wait_for_http,
)


def upload(client, repo, payload):
    digest = content_digest(payload)
    location = client.start_blob_upload(repo)
    location = client.upload_blob_chunk(location, payload)
    client.finish_blob_upload(location, digest)
    return {"digest": digest, "size": len(payload)}


def image(client, repo, layer):
    config = upload(client, repo, canonical_bytes({"architecture": "amd64", "os": "linux",
        "rootfs": {"type": "layers", "diff_ids": []}, "config": {}}))
    config["mediaType"] = "application/vnd.oci.image.config.v1+json"
    layer = upload(client, repo, layer) | {"mediaType": "application/vnd.oci.image.layer.v1.tar"}
    manifest = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
                                "config": config, "layers": [layer]})
    client.put_manifest(repo, "latest", manifest, media_type=OCI_IMAGE)
    return content_digest(manifest), layer["digest"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-hostname", required=True)
    args = parser.parse_args()
    if os.geteuid() != 0 or socket.gethostname() != args.expected_hostname:
        parser.error("requires root on the explicitly named disposable host")
    config = DeploymentConfig.from_file(args.config)
    url = f"http://127.0.0.1:{config.registry_port}"
    wait_for_http("fixture registry", url + "/v2/")
    client = RegistryClient(url)
    if client.catalog():
        parser.error("qualification requires an empty disposable registry")
    result = {"passed": False, "kernel": os.uname().release,
              "scope": "real Distribution 3.1.1 and systemd; isolated Hetzner VM"}
    try:
        keep, layer = image(client, "kept", b"kept fixture content")
        dead, discarded = image(client, "discarded", b"discarded fixture content")
        client.delete_manifest("discarded", dead)
        tree = DistributionTree(config.registry_data_dir())
        stamp = time.time() - config.registry_blob_grace_seconds - 3600
        for path in tree.v2.rglob("*"):
            if path.is_file():
                os.utime(path, (stamp, stamp))
        started = time.monotonic()
        sweep = run_registry_sweep(config=config, lock_file=REGISTRY_MAINTENANCE_LOCK)
        wait_for_http("registry after collection", url + "/v2/")
        result["collection_and_ready_seconds"] = time.monotonic() - started
        result["collection"] = sweep.to_dict()
        assert client.blob_bytes("kept", layer, max_bytes=1024) == b"kept fixture content"
        assert content_digest(canonical_bytes(client.manifest_document("kept", keep)[0])) == keep
        assert not (tree.blob_dir(discarded) / "data").exists()
        image(client, "after-collection", b"pushes work again")
        result["live_reads_and_new_pushes_survive"] = True

        def fail(*args, **kwargs):
            raise RuntimeError("injected collector failure")

        try:
            run_registry_sweep(config=config, lock_file=REGISTRY_MAINTENANCE_LOCK, sweep=fail)
        except RuntimeError as exc:
            assert str(exc) == "injected collector failure"
        else:
            raise AssertionError("collector failure was swallowed")
        wait_for_http("registry after failure", url + "/v2/")
        result["exception_restarts_registry"] = True

        # Exercise the actual packaged ExecStopPost after SIGKILL, where Python's
        # finally block cannot run. Never install this override on a real fleet.
        crash = args.output.with_suffix(".crash.py")
        crash.write_text("import os, signal\nfrom ucloud_sandboxes.systemd import stopped_registry\n"
                         "with stopped_registry():\n    os.kill(os.getpid(), signal.SIGKILL)\n")
        override = Path("/etc/systemd/system/ucloud-sandbox-registry-gc.service.d/qualification.conf")
        override.parent.mkdir(parents=True, exist_ok=True)
        try:
            override.write_text(f"[Service]\nExecStart=\nExecStart={sys.executable} {crash}\n")
            subprocess.run(["systemctl", "daemon-reload"], check=True)
            failed = subprocess.run(["systemctl", "start", "ucloud-sandbox-registry-gc.service"], capture_output=True)
            assert failed.returncode != 0
            wait_for_http("registry after collector SIGKILL", url + "/v2/")
            assert not registry_restart_marker(REGISTRY_WRITER_LOCK).exists()
            assert client.blob_bytes("kept", layer, max_bytes=1024) == b"kept fixture content"
            result["sigkill_recovery_preserves_live_blob"] = True
        finally:
            override.unlink(missing_ok=True)
            crash.unlink(missing_ok=True)
            subprocess.run(["systemctl", "daemon-reload"], check=True)
            subprocess.run(["systemctl", "reset-failed", "ucloud-sandbox-registry-gc.service"], check=True)
        result["passed"] = True
    except Exception as exc:
        result["error"] = repr(exc)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
