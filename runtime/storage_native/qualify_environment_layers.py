"""Shared-layer Linux scenario, invoked by qualify_environment.py --layers.

Uses Docker's real overlay2 diffs as both the builder input and reference tree.
Only fixture-owned image tags, mounts and directories are collected.
"""
from contextlib import ExitStack
import copy
import hashlib
import os
from pathlib import Path
import shutil
import stat
import subprocess
import time

from ucloud_sandboxes.environment_artifact import (
    OCI_IMAGE, attach_environment_to_image, canonical_bytes,
    publish_environment,
)
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder, WHOLE_IMAGE_EXCLUDED
from ucloud_sandboxes.environment_config import configured_environment_registry
from ucloud_sandboxes.environment_manifest import EnvironmentManifest
from ucloud_sandboxes.environment_rootfs import EnvironmentRootfsStore
from ucloud_sandboxes.image_rootfs import DockerOverlay2RootfsStore, OverlayRootfsManager


def tree_identity(root):
    """Compare visible contents, ownership, modes, user/capability xattrs and links.

    EROFS deliberately normalizes timestamps; overlay implementation xattrs and
    synthetic runtime mounts are not application filesystem contents.
    """
    entries, hardlinks = {}, {}
    for directory, dirs, files in os.walk(root, followlinks=False):
        if Path(directory) == root:
            dirs[:] = [name for name in dirs if name not in WHOLE_IMAGE_EXCLUDED]
            files = [name for name in files if name not in WHOLE_IMAGE_EXCLUDED]
        for name in sorted(dirs + files):
            path = Path(directory) / name
            relative = str(path.relative_to(root))
            info = path.lstat()
            entry = {"mode": info.st_mode, "uid": info.st_uid, "gid": info.st_gid}
            entry["xattrs"] = {key: os.getxattr(path, key, follow_symlinks=False).hex()
                               for key in os.listxattr(path, follow_symlinks=False)
                               if key.startswith("user.") or key == "security.capability"}
            if stat.S_ISREG(info.st_mode):
                digest = hashlib.sha256()  # hashlib.file_digest needs Python 3.11.
                with path.open("rb") as source:
                    while block := source.read(1 << 20):
                        digest.update(block)
                entry["sha256"] = digest.hexdigest()
                hardlinks.setdefault((info.st_dev, info.st_ino), []).append(relative)
            elif stat.S_ISLNK(info.st_mode):
                entry["target"] = os.readlink(path)
            elif not stat.S_ISDIR(info.st_mode):
                entry["device"] = info.st_rdev
            entries[relative] = entry
    return {"entries": entries, "hardlinks": sorted(sorted(paths) for paths in hardlinks.values() if len(paths) > 1)}


def qualify_layers(root, source, fixture, registry, key, backend, url, runsc, config):
    work = root / "layer-qualification"
    work.mkdir()
    prefix = "ucloud-layer-qualification-" + root.name.rsplit("-", 1)[-1]
    tags = [prefix + ":" + name for name in ("base", "task-a", "task-b")]
    docker = DockerOverlay2RootfsStore(work / "docker")
    builder = FreshEnvironmentBuilder(docker, registry, key, work / "builder")
    worker = EnvironmentRootfsStore(work / "worker", configured_environment_registry(
        url, "environments", root / "keys.json"), backend)
    receipts, references, identities, mounted_ids = [], [], [], []
    started = time.monotonic()
    try:
        context = work / "context"
        shutil.copytree(source, context / "root", symlinks=True)
        # COPY does not promise to preserve source hardlinks; RUN explicitly
        # creates links in a Docker diff and exercises the squasher's handling.
        recipes = ["FROM scratch\nCOPY root/ /\n",
                   f"FROM {tags[0]}\n"
                   "RUN [\"/bin/busybox\", \"sh\", \"-ec\", \"/bin/busybox rm /delete-me; /bin/busybox rm -rf /opaque; /bin/busybox mkdir /opaque; echo new > /opaque/new; /bin/busybox ln /etc/hello /etc/linked; /bin/busybox mkdir /temporary; echo gone > /temporary/gone\"]\n"
                   "RUN [\"/bin/busybox\", \"sh\", \"-ec\", \"/bin/busybox rm /temporary/gone; echo changed > /etc/hello; /bin/busybox ln -s hello /etc/other-link\"]\n",
                   f"FROM {tags[0]}\n"
                   "RUN [\"/bin/busybox\", \"sh\", \"-ec\", \"/bin/busybox rm -rf /opaque; echo file > /opaque; echo task-b > /etc/hello\"]\n"
                   "RUN [\"/bin/busybox\", \"sh\", \"-ec\", \"/bin/busybox rm /opaque; /bin/busybox mkdir /opaque; echo replacement > /opaque/replacement\"]\n"]
        for tag, recipe in zip(tags, recipes):
            (context / "Dockerfile").write_text(recipe)
            subprocess.run(["docker", "build", "--network=none", "-t", tag, str(context)],
                           check=True, capture_output=True, timeout=300,
                           env={**os.environ, "DOCKER_BUILDKIT": "0"})
            with docker.operation_lease(tag) as image:
                references.append(tree_identity(image.rootfs))
                image_id, diffs, directories = docker.layer_diffs(tag)
                # This local registry fixture has no compressed OCI tars. Use
                # diff sizes for planning; the random 64 MiB base closes its
                # own group, while both task layers squash into one group.
                fixture.layer_sizes[tag] = [sum(p.lstat().st_size for p in directory.rglob("*")
                                                if p.is_file() and not p.is_symlink())
                                           for directory in directories]
            result = builder.build_layers(tag, repository="environments", reference=tag)
            assert result is not None
            components = result["components"]
            environment = publish_environment(registry, source_image=image_id,
                environment=EnvironmentManifest(components[0], toolkits=tuple(components[1:])),
                image_config={"Cmd": ["/bin/busybox", "sh"]}, signing_key=key,
                tag=tag + "-root", source_diff_ids=diffs)
            fixture.put_manifest("environments", tag, canonical_bytes({"schemaVersion": 2,
                "mediaType": OCI_IMAGE, "config": {"digest": image_id}, "layers": []}), media_type=OCI_IMAGE)
            annotated = attach_environment_to_image(registry, image_repository="environments",
                image_reference=tag, environment_digest=environment)
            receipts.append({"tag": tag, "components": components, "reused": result["reused"],
                             "reference": url[7:] + "/environments@" + annotated})
            identities.append(image_id)
        shared = receipts[0]["components"][0]
        assert all(item["components"][0] == shared for item in receipts)
        assert all(item["reused"] >= 1 for item in receipts[1:])
        # Keep all compositions mounted simultaneously: this exercises the
        # repeated null-UUID EROFS superblocks and shared-device GC fences.
        with ExitStack() as leases:
            for receipt, expected in zip(receipts, references):
                image = leases.enter_context(worker.operation_lease(receipt["reference"]))
                mounted_ids.append(image.image_id)
                actual = tree_identity(image.rootfs)
                assert actual == expected, {"image": receipt["tag"], "different_paths": [
                    p for p in sorted(set(actual["entries"]) | set(expected["entries"]))
                    if actual["entries"].get(p) != expected["entries"].get(p)],
                    "hardlinks": [actual["hardlinks"], expected["hardlinks"]]}
            assert not backend.drop(shared)
        unique = set(component for item in receipts for component in item["components"])
        snapshot = worker.operation_snapshot()
        assert snapshot["environment_devices_in_use"] == len(unique)
        # Exercise a real guest and its writable overlay on the v2 composition,
        # not only host-side directory reads or the earlier v1 guest.
        manager = OverlayRootfsManager(worker, writable_root=work / "writable", bundle_root=work / "bundles")
        guest_config = copy.deepcopy(config)
        guest_config["process"]["args"] = ["/bin/busybox", "sh", "-ec",
            'test "$(/bin/busybox cat /etc/hello)" = changed; '
            'test ! -e /delete-me; test ! -e /opaque/old; test -e /opaque/new; '
            'echo writable > /etc/hello; test "$(/bin/busybox cat /etc/hello)" = writable; '
            'echo LAYER_GUEST_OK']
        with worker.operation_lease(receipts[1]["reference"]) as image:
            lease = manager.prepare(sandbox_id="layer-probe", sandbox_generation=1,
                                    image=image, config_template=guest_config)
        try:
            guest_runsc = ["--application-memory-file-dir=" + str(work / "writable")
                           if item.startswith("--application-memory-file-dir=") else item for item in runsc]
            guest = subprocess.run(guest_runsc + ["run", "--bundle=" + str(lease.sandbox.bundle),
                                           lease.sandbox.container_id],
                                   capture_output=True, text=True, timeout=30)
            assert guest.returncode == 0 and "LAYER_GUEST_OK" in guest.stdout, guest.stderr
        finally:
            subprocess.run(runsc + ["delete", "--force", lease.sandbox.container_id],
                           capture_output=True, timeout=20)
            manager.release(lease)
        # Rebuilding an already published image must upload no new blobs.
        before = set(fixture.blobs)
        repeated = builder.build_layers(tags[1], repository="environments", reference=tags[1])
        assert repeated["components"] == receipts[1]["components"]
        assert repeated["reused"] == len(repeated["components"])
        assert set(fixture.blobs) == before
        # Collecting one image must not disconnect its siblings' shared base.
        assert worker.collect_image(mounted_ids.pop(0), is_referenced=lambda _: False)
        assert not backend.drop(shared)
        for receipt, expected in zip(receipts[1:], references[1:]):
            with worker.operation_lease(receipt["reference"]) as image:
                assert tree_identity(image.rootfs) == expected
        return {"passed": True, "images": 3, "docker_reference_trees_match": True,
                "layered_live_guest_and_copyup": True,
                "shared_base_gc_fenced": True, "republication_uploads_no_blobs": True,
                "component_references": sum(len(item["components"]) for item in receipts),
                "unique_components": len(unique), "seconds": time.monotonic() - started,
                "component_image_bytes": sum(registry.load(d).image_size for d in unique),
                "without_sharing_image_bytes": sum(registry.load(d).image_size
                    for item in receipts for d in item["components"])}
    finally:
        for image_id in mounted_ids:
            worker.collect_image(image_id, is_referenced=lambda _: False)
        for image_id in identities:
            docker.collect_image(image_id, is_referenced=lambda _: False)
        subprocess.run(["docker", "image", "rm", *tags], capture_output=True, timeout=60)
