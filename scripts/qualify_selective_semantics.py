#!/usr/bin/env python3
"""Create bounded, owned OCI semantic canaries and compare both EROFS paths.

Run only on the selected idle builder with the candidate on PYTHONPATH. Existing
source tags are untouched. New qual-* source tags and canonical Docker-produced
EROFS groups are retained as evidence; there is no registry deletion or pruning.
New layer/config uploads are bounded to 2 MiB total. Base blobs are referenced
in their existing repository. Signing credentials remain on the builder.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import time
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlsplit
from uuid import uuid4

CASES = ("links", "whiteout-opaque", "missing-parent", "cross-layer-hardlink")
EPOCH = 1700000000


def sha(payload):
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def entry(name, kind="file", body=b"", *, link="", mode=0o640, uid=23123, gid=23124):
    return {"name": name, "kind": kind, "body": body, "link": link, "mode": mode, "uid": uid, "gid": gid}


def directory(name, **kwargs):
    return entry(name, "directory", mode=0o750, **kwargs)


def layer(entries):
    archive = io.BytesIO()
    proof = []
    with tarfile.open(fileobj=archive, mode="w", format=tarfile.USTAR_FORMAT) as stream:
        for item in entries:
            member = tarfile.TarInfo(item["name"])
            member.type = {"file": tarfile.REGTYPE, "directory": tarfile.DIRTYPE,
                           "hardlink": tarfile.LNKTYPE, "symlink": tarfile.SYMTYPE}[item["kind"]]
            member.mode, member.uid, member.gid = item["mode"], item["uid"], item["gid"]
            member.mtime, member.linkname = EPOCH, item["link"]
            member.size = len(item["body"]) if item["kind"] == "file" else 0
            stream.addfile(member, io.BytesIO(item["body"]) if member.size else None)
            proof.append({key: value for key, value in item.items() if key != "body"}
                         | {"size": member.size, "mtime": EPOCH, "content_sha256": sha(item["body"])})
    raw = archive.getvalue()
    compressed = gzip.compress(raw, mtime=0)
    if len(compressed) > 64 * 1024 or len(raw) > 128 * 1024:
        raise ValueError("Semantic fixture exceeds its tiny-layer bound")
    return {"payload": compressed, "diff_id": sha(raw), "tar_bytes": len(raw), "members": proof,
            "descriptor": {"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                           "digest": sha(compressed), "size": len(compressed)}}


def fixture_layers(prefix):
    lower = layer([directory(prefix), directory(prefix + "/delete"),
                   entry(prefix + "/delete/gone", body=b"must disappear\n"), directory(prefix + "/opaque"),
                   entry(prefix + "/opaque/old", body=b"must be hidden\n"), directory(prefix + "/implicit"),
                   entry(prefix + "/hard-target", body=b"lower-layer hardlink target\n")])
    links = layer([directory(prefix), directory(prefix + "/links"),
                   entry(prefix + "/links/executable", body=b"#!/bin/sh\necho semantic-canary\n", mode=0o751),
                   entry(prefix + "/links/hard", "hardlink", link=prefix + "/links/executable", mode=0o751),
                   entry(prefix + "/links/relative", "symlink", link="executable", mode=0o777),
                   entry(prefix + "/links/dangling", "symlink", link="../missing", mode=0o777),
                   entry(prefix + "/links/absolute", "symlink", link="/missing/qualification-target", mode=0o777)])
    whiteouts = layer([directory(prefix), directory(prefix + "/delete"), directory(prefix + "/opaque"),
                       entry(prefix + "/delete/.wh.gone"), entry(prefix + "/opaque/.wh..wh..opq"),
                       entry(prefix + "/opaque/new", body=b"replacement visible\n")])
    missing = layer([directory(prefix), entry(prefix + "/implicit/child", body=b"inherited directory metadata\n")])
    cross = layer([directory(prefix), entry(prefix + "/cross", "hardlink", link=prefix + "/hard-target")])
    return lower, {"links": links, "whiteout-opaque": whiteouts, "missing-parent": missing,
                   "cross-layer-hardlink": cross}


def append_documents(base, config, layers):
    document, image_config = deepcopy(base), deepcopy(config)
    if len(image_config["rootfs"]["diff_ids"]) != len(document["layers"]):
        raise ValueError("Base source config/layers differ")
    image_config["rootfs"]["diff_ids"].extend(item["diff_id"] for item in layers)
    image_config.setdefault("history", []).extend({"created": "2023-11-14T22:13:20Z",
        "created_by": "owned selective EROFS semantic qualification"} for _ in layers)
    raw_config = json.dumps(image_config, sort_keys=True, separators=(",", ":")).encode()
    if len(raw_config) > 256 * 1024:
        raise ValueError("Canary config exceeds 256 KiB")
    document.pop("annotations", None)  # Never inherit another image's signed environment root.
    document["config"] = {"mediaType": base["config"]["mediaType"], "size": len(raw_config), "digest": sha(raw_config)}
    document["layers"].extend(item["descriptor"] for item in layers)
    raw_manifest = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return raw_config, raw_manifest


def run(args):
    # Use the sibling reviewed helper whether run from the repo or a staged directory.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from qualify_selective_environment import compare, signing_key
    from ucloud_sandboxes.environment_artifact import EnvironmentArtifactRegistry
    from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder, publication_metrics
    from ucloud_sandboxes.environment_config import load_trusted_keys
    from ucloud_sandboxes.image_rootfs import DockerOverlay2RootfsStore
    from ucloud_sandboxes.managed_registry import RegistryClient, RegistryRequestError

    url = urlsplit(args.registry_url)
    if url.scheme not in {"http", "https"} or not url.netloc or url.username or url.password or url.query or url.fragment:
        raise ValueError("Expected a plain registry URL")
    if not re.fullmatch(r"ucloud-managed/[a-z0-9]+(?:[._-][a-z0-9]+)*", args.repository):
        raise ValueError("Reuse an existing owned benchmark repository under ucloud-managed/")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", args.manifest):
        raise ValueError("A pinned source manifest is required")
    args.work_root.mkdir(mode=0o700, parents=True, exist_ok=False)
    run_id = uuid4().hex[:16]
    client = RegistryClient(args.registry_url)
    base, _ = client.manifest_document(args.repository, args.manifest)
    descriptor = base["config"]
    if not 0 < descriptor["size"] <= 256 * 1024:
        raise ValueError("Base config exceeds qualification bound")
    raw = client.blob_bytes(args.repository, descriptor["digest"], max_bytes=descriptor["size"])
    if sha(raw) != descriptor["digest"] or len(raw) != descriptor["size"]:
        raise ValueError("Base config identity mismatch")
    config = json.loads(raw)
    lower, fixtures = fixture_layers("ucloud-qual-" + run_id)
    trust, key = load_trusted_keys(args.trusted_keys), signing_key(args.signing_key)
    receipt = {"run_id": run_id, "started_at": datetime.now(timezone.utc).isoformat(),
               "source_repository": args.repository, "base_manifest": args.manifest,
               "new_blob_bytes_uploaded": 0, "max_new_blob_bytes": 2 * 1024**2,
               "registry_cleanup": "Owned qual-* source tags and Docker-produced EROFS groups retained; no GC or deletes.",
               "cases": [], "complete": False}
    uploaded = set()

    def upload(payload, identity):
        if identity in uploaded:
            return
        if receipt["new_blob_bytes_uploaded"] + len(payload) > receipt["max_new_blob_bytes"]:
            raise ValueError("New upload byte budget exceeded")
        path = args.work_root / (identity[7:] + ".blob")
        path.write_bytes(payload)
        client.upload_blob_file(args.repository, path, identity, len(payload))
        uploaded.add(identity)
        receipt["new_blob_bytes_uploaded"] += len(payload)

    try:
        for case in args.cases:
            tag = "qual-" + run_id + "-" + case
            try:
                client.manifest_document(args.repository, tag)
            except RegistryRequestError as exc:
                if exc.status_code != 404:
                    raise
            else:
                raise ValueError("Refusing to overwrite an existing qualification tag")
            selected = [lower, fixtures[case]]
            raw_config, raw_manifest = append_documents(base, config, selected)
            record = {"case": case, "tag": tag, "expected_path": "selective" if case == "links" else "fallback",
                      "manifest": sha(raw_manifest), "tar_proof": [{k: v for k, v in value.items() if k != "payload"}
                                                                   for value in selected]}
            receipt["cases"].append(record)
            for value in selected:
                upload(value["payload"], value["descriptor"]["digest"])
            upload(raw_config, sha(raw_config))
            client.put_manifest(args.repository, tag, raw_manifest, media_type=base["mediaType"])
            pinned = url.netloc + "/" + args.repository + "@" + record["manifest"]
            store_root = args.work_root / (case + "-baseline")
            store = DockerOverlay2RootfsStore(store_root / "images", docker_binary=args.docker)
            builder = FreshEnvironmentBuilder(store, EnvironmentArtifactRegistry(client, args.environment_repository, trust),
                                              key, store_root / "scratch")
            original = builder._reuse_layer_component
            # Baseline always uses the existing Docker extraction path. Suppress
            # refresh of existing shared components; publish only genuine misses.
            def lookup(tag, *values, **_kwargs):
                return original(tag, *values, refresh=False)

            started = time.monotonic()
            try:
                with patch.object(builder, "_reuse_layer_component", side_effect=lookup), \
                        patch.object(builder, "_materialize_registry_groups", return_value=None), publication_metrics() as metrics:
                    store._checked(args.docker, "pull", pinned, timeout=600)
                    result = builder.build_layers(pinned, repository=args.repository, reference=record["manifest"])
                if result is None:
                    raise ValueError("Baseline Docker path did not produce layer components")
                record.update(baseline_metrics=dict(metrics), baseline_seconds=time.monotonic() - started,
                              baseline_components=result["components"])
                compare(SimpleNamespace(registry_url=args.registry_url, environment_repository=args.environment_repository,
                    repository=args.repository, manifest=record["manifest"], work_root=args.work_root / (case + "-comparison"),
                    group=-1, order="AB", expect=record["expected_path"], trusted_keys=args.trusted_keys,
                    signing_key=args.signing_key, docker=args.docker))
                record["equivalent"] = True
            finally:
                # Remove only this unique canary's local immutable image reference.
                # No force flag, broad Docker prune, shared base removal, or registry delete.
                cleanup = subprocess.run([args.docker, "image", "rm", "--no-prune", pinned], capture_output=True, timeout=60)
                record["local_image_cleanup_returncode"] = cleanup.returncode
        receipt["complete"] = True
    finally:
        receipt["finished_at"] = datetime.now(timezone.utc).isoformat()
        (args.work_root / "semantic-canaries.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"complete": True, "cases": len(receipt["cases"]),
                      "new_blob_bytes_uploaded": receipt["new_blob_bytes_uploaded"], "receipt": str(args.work_root / "semantic-canaries.json")}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", default="http://10.42.0.2:5000")
    parser.add_argument("--environment-repository", default="environments")
    parser.add_argument("--repository", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    parser.add_argument("--trusted-keys", type=Path, default=Path("/etc/ucloud-sandboxes/environment/producers.json"))
    parser.add_argument("--signing-key", type=Path, default=Path("/etc/ucloud-sandboxes/environment/producer.pem"))
    parser.add_argument("--docker", default="docker")
    args = parser.parse_args()
    if len(args.cases) != len(set(args.cases)):
        parser.error("Cases must be unique")
    run(args)


if __name__ == "__main__":
    main()
