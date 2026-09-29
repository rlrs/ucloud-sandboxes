#!/usr/bin/env python3
"""Compare selective EROFS materialization with Docker on one immutable image.

Run only on an owned idle builder using the candidate package. Registry access
is read-only: forced misses and published components exist only in this process.
Docker pulls/mounts and temporary files are local to this qualification. No tag
refresh, image annotation, registry deletion, or cache pruning is performed.
"""
from __future__ import annotations

import argparse
from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import stat
import time
from unittest.mock import patch
from urllib.parse import urlsplit


class ReadOnlyClient:
    """Fail closed if the candidate tries to write through the registry client."""

    READS = frozenset({"manifest_document", "manifest_layers", "blob_bytes", "open_blob", "blob_exists",
                       "base_url", "timeout_seconds"})

    def __init__(self, client):
        self._client = client

    def __getattr__(self, name):
        if name not in self.READS:
            raise RuntimeError("Qualification blocked a registry operation: " + name)
        return getattr(self._client, name)


def file_digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return "sha256:" + digest.hexdigest()


def capture_registry(client, repository, trusted_keys):
    from ucloud_sandboxes.environment_artifact import EnvironmentArtifactRegistry, canonical_bytes, content_digest

    class CapturedRegistry(EnvironmentArtifactRegistry):
        def __init__(self):
            super().__init__(ReadOnlyClient(client), repository, trusted_keys)
            self.captured = {}
            self.publications = []

        def publish(self, image, component, *, tag):
            component.authenticate(self.trusted_keys)
            if image.stat().st_size != component.image_size or file_digest(image) != component.image_digest:
                raise ValueError("Locally captured EROFS bytes differ from their signed component")
            # This is a local handle, deliberately not a claim of OCI publication.
            handle = content_digest(b"qualification-local-component\0" + canonical_bytes(component.to_dict()))
            self.captured[handle] = component
            self.publications.append({"tag": tag, "component": component.to_dict(), "bytes_rehashed": True})
            return handle

        def load(self, digest):
            return self.captured[digest] if digest in self.captured else super().load(digest)

    return CapturedRegistry()


def signing_key(path):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("Signing key must be a private owned regular file")
        payload = stream.read(16 * 1024 + 1)
    if len(payload) > 16 * 1024:
        raise ValueError("Signing key exceeds its size bound")
    key = load_pem_private_key(payload, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("Expected an Ed25519 signing key")
    return key


def compare(args):
    from ucloud_sandboxes import environment_builder
    from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder, MAX_LAYER_GROUPS, publication_metrics
    from ucloud_sandboxes.environment_config import load_trusted_keys
    from ucloud_sandboxes.image_rootfs import DockerOverlay2RootfsStore
    from ucloud_sandboxes.managed_registry import RegistryClient

    url = urlsplit(args.registry_url)
    if url.scheme not in {"http", "https"} or not url.netloc or url.username or url.password or url.query or url.fragment:
        raise ValueError("Expected a plain registry URL without embedded credentials")
    if not re.fullmatch(r"[a-z0-9]+(?:[._/-][a-z0-9]+)*", args.repository):
        raise ValueError("Invalid source repository")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", args.manifest):
        raise ValueError("An immutable source manifest digest is required")
    args.work_root.mkdir(mode=0o700, parents=True, exist_ok=False)
    client = RegistryClient(args.registry_url)
    trust, key = load_trusted_keys(args.trusted_keys), signing_key(args.signing_key)
    image_ref = url.netloc + "/" + args.repository + "@" + args.manifest
    output = {"started_at": datetime.now(timezone.utc).isoformat(), "source_repository": args.repository,
              "source_manifest": args.manifest, "expected_candidate_path": args.expect,
              "source_sha256": "sha256:" + hashlib.sha256(inspect.getsource(environment_builder).encode()).hexdigest(), "order": args.order,
              "registry_writes": 0, "trials": [], "equivalent": False,
              "timing_limit": "Local Docker cache warms across trials; this is byte qualification, not end-to-end build latency."}

    def builder_at(root):
        registry = capture_registry(client, args.environment_repository, trust)
        return FreshEnvironmentBuilder(DockerOverlay2RootfsStore(root / "images", docker_binary=args.docker),
                                       registry, key, root / "scratch",
                                       preparation_subprocess=getattr(args, "preparation_subprocess", False))

    builder = builder_at(args.work_root / "probe")
    original, seen = builder._reuse_layer_component, []

    def probe_lookup(tag, *values, **_kwargs):
        if tag not in seen:
            seen.append(tag)
        return original(tag, *values, refresh=False)

    # The input must already have every group published by the existing path.
    # Suppress selective materialization here so a cold input cannot be primed
    # by the code being qualified.
    with patch.object(builder, "_reuse_layer_component", side_effect=probe_lookup), \
            patch.object(builder, "_materialize_registry_groups", return_value=None):
        warm = builder._reuse_image_layers(args.repository, args.manifest, max_groups=MAX_LAYER_GROUPS)
    if warm is None or len(seen) < 2:
        raise ValueError("Expected an already published image with at least two cached EROFS groups")
    index = args.group if args.group >= 0 else len(seen) + args.group
    if not 0 < index < len(seen):
        raise ValueError("Select a non-base cached group to exercise reuse of its lower groups")
    selected = seen[index]
    output.update(group_index=index, group_count=len(seen), forced_missing_tag=selected,
                  format=builder.layer_format(), source_image_id=warm["image_id"])
    reference_components = [builder.registry.load(digest).to_dict() for digest in warm["components"]]
    output["reference_component_digests"] = [item["image_digest"] for item in reference_components]
    try:
        for ordinal, arm in enumerate(args.order):
            builder = builder_at(args.work_root / f"trial-{ordinal}-{arm}")
            original = builder._reuse_layer_component

            def lookup(tag, *values, **_kwargs):
                return None if tag == selected else original(tag, *values, refresh=False)

            disable = patch.object(builder, "_materialize_registry_groups", return_value=None) if arm == "A" else nullcontext()
            started = time.monotonic()
            docker_used = False
            with patch.object(builder, "_reuse_layer_component", side_effect=lookup), disable, publication_metrics() as metrics:
                result = builder._reuse_image_layers(args.repository, args.manifest, max_groups=MAX_LAYER_GROUPS)
                if result is None:
                    docker_used = True
                    builder.image_store._checked(args.docker, "pull", image_ref, timeout=600)
                    result = builder.build_layers(image_ref, repository=args.repository, reference=args.manifest)
            if result is None:
                raise ValueError("Source image cannot be qualified as layer components")
            components = [builder.registry.load(digest).to_dict() for digest in result["components"]]
            trial = {"ordinal": ordinal, "arm": arm, "wall_seconds": time.monotonic() - started,
                     "docker_used": docker_used, "metrics": dict(metrics), "components": components,
                     "captured_publications": builder.registry.publications,
                     "matches_published_reference": components == reference_components}
            output["trials"].append(trial)
            if result["image_id"] != warm["image_id"] or tuple(result["diff_ids"]) != tuple(warm["diff_ids"]):
                raise ValueError("Image identity or source layers changed during comparison")
            if result["image_config"] != warm["image_config"]:
                raise ValueError("Runtime image configuration differs between paths")
            if len(builder.registry.publications) != 1 or builder.registry.publications[0]["tag"] != selected:
                raise ValueError("Expected exactly the selected missing group to be materialized")
            if components != reference_components:
                raise ValueError("EROFS bytes or signed metadata differ from the published Docker reference")
            if arm == "B" and ((args.expect == "selective") == docker_used):
                raise ValueError("Candidate did not take the required selective/fallback path")
            if arm == "B" and args.expect == "selective" and metrics.get("selective_materializations", 0) != 1:
                raise ValueError("Missing positive evidence that selective materialization ran")
            if (arm == "B" and args.expect == "selective"
                    and getattr(args, "preparation_subprocess", False)
                    and metrics.get("selective_subprocess_ms", 0) <= 0):
                raise ValueError("Missing positive evidence that isolated preparation ran")
        output["equivalent"] = True
    finally:
        output["finished_at"] = datetime.now(timezone.utc).isoformat()
        (args.work_root / "comparison.json").write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps({"equivalent": True, "trials": len(output["trials"]), "registry_writes": 0,
                      "output": str(args.work_root / "comparison.json")}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", default="http://10.42.0.2:5000")
    parser.add_argument("--environment-repository", default="environments")
    parser.add_argument("--repository", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--work-root", type=Path, required=True, help="New owned directory; must not already exist")
    parser.add_argument("--group", type=int, default=-1)
    parser.add_argument("--order", choices=("AB", "ABBA"), default="ABBA")
    parser.add_argument("--expect", choices=("selective", "fallback"), required=True)
    parser.add_argument("--preparation-subprocess", action="store_true",
                        help="Exercise the production isolated selective preparation path")
    parser.add_argument("--trusted-keys", type=Path, default=Path("/etc/ucloud-sandboxes/environment/producers.json"))
    parser.add_argument("--signing-key", type=Path, default=Path("/etc/ucloud-sandboxes/environment/producer.pem"))
    parser.add_argument("--docker", default="docker")
    compare(parser.parse_args())


if __name__ == "__main__":
    main()
