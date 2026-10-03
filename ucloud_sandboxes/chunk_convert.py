"""Convert OCI images from our registry into the chunk store (design §3).

Per layer: ``nydus-image create`` (v2.4.5, RAFS v6, sha256, zstd, 256 KiB,
no ``--repeatable``, which drops owners, no chunk dictionary), then the chunks the index does not
know are verified and packed, the packs PUT, and one commit records them.
Per image: the signed component(s), registration, and last the root
manifest. Every object is content addressed and the root is the only
visible commit point, so a crash at any step leaves nothing visible and a
rerun converges on the same root digest.
"""
from __future__ import annotations

from contextlib import contextmanager
import copy
from dataclasses import dataclass, field
import gzip
import hashlib
import json
import logging
import os
import shutil
from pathlib import Path
import socket
import stat
import subprocess
import tarfile
from tempfile import TemporaryDirectory
import time

from .chunk_store import (BLOCK, MAX_BOOTSTRAP_BYTES, Locator, PackWriter, bootstrap_key, chunk_map_from_bootstrap,
                          chunk_map_key, decode_chunk, pack_key, parse_bootstrap, store_encoding,
                          zstd_compress, zstd_content_size, zstd_decompress)
from .environment_artifact import (EMPTY_LAYER_DIFF_ID, ENVIRONMENT_ANNOTATION, OCI_IMAGE, RAFS_CONVERTER,
                                   RafsEnvironmentComponent, canonical_bytes, content_digest, load_environment,
                                   publish_environment, require_digest, sign_rafs_component)
from .erofs_metadata import symlink_targets

_LOG = logging.getLogger(__name__)
DOCKER_MANIFEST = "application/vnd.docker.distribution.manifest.v2+json"
STEPS = ("layer_converted", "pack_put", "pack_committed", "layer_bootstrap_put", "layer_committed", "image_merged",
         "metadata_put", "component_signed", "verified", "component_published", "registered", "root_published")
_OPAQUE = ".wh..wh..opq"
_OVERLAY_OPAQUE = "SCHILY.xattr.trusted.overlay.opaque"
MAX_LAYERS_PER_ROOT = 33  # The base and 32 toolkits (ImmutableEnvironment).
LOOKUP_BATCH = 256  # Chunk ids per index lookup while packing: about one 64 MiB pack.


def converter_identity(layout):
    """Everything besides the layer that decides a layer bootstrap's bytes."""
    whiteouts = "oci" if layout == "image" else "overlayfs"
    return f"{RAFS_CONVERTER};fs6;sha256;zstd;0x40000;owners;whiteouts={whiteouts};order=path"  # never --repeatable: it zeroes owners


def strip_environment_annotation(document):
    """A manifest derived from an annotated one must not name its old root
    (nydusify copies source annotations, S10)."""
    annotations = {name: value for name, value in (document.get("annotations") or {}).items()
                   if name != ENVIRONMENT_ANNOTATION}
    stripped = {name: value for name, value in document.items() if name != "annotations"}
    return stripped | ({"annotations": annotations} if annotations else {})


def _path_order(member):
    name = _normal(member.name)
    return tuple(name.split("/")) if name else ()


def path_ordered_layer(source, destination):
    """Rewrite a layer tar depth-first in path order, unless it already is:
    nydus-image tar-rafs drops whiteouts of a layer that returns to a
    directory it left (OpenSWE's slim layers, M1 gate). A hardlink sorted
    before its target carries the data and the target links to it. Returns
    False, writing nothing, for a tar already in order (docker, BuildKit)."""
    with tarfile.open(source, "r:*") as reader:
        members = reader.getmembers()
        keys = [_path_order(member) for member in members]
        if all(left <= right for left, right in zip(keys, keys[1:])):
            return False
        plain = Path(destination).with_suffix(".plain")  # Seeking a compressed tar re-reads it.
        reader.fileobj.seek(0)
        with open(plain, "wb") as stream:
            shutil.copyfileobj(reader.fileobj, stream, 1 << 20)
    try:
        with tarfile.open(plain, "r:") as reader, tarfile.open(destination, "w", format=tarfile.PAX_FORMAT) as writer:
            members, latest, target = reader.getmembers(), {}, {}
            for member in members:  # A hardlink names the file written before it.
                if member.islnk():
                    target[id(member)] = latest.get(_normal(member.linkname))
                elif member.isfile():
                    latest[_normal(member.name)] = member
            primary = {}  # id(file member) -> the name its data went out under
            for member in sorted(members, key=_path_order):
                source_file = target.get(id(member)) if member.islnk() else member if member.isfile() else None
                if source_file is not None and id(source_file) in primary:
                    if member is source_file:
                        member = copy.copy(member)
                        member.type, member.size = tarfile.LNKTYPE, 0
                    member.linkname = primary[id(source_file)]
                    writer.addfile(member)
                elif source_file is not None:
                    primary[id(source_file)] = member.name
                    data = copy.copy(source_file)
                    data.name = member.name
                    writer.addfile(data, reader.extractfile(source_file))
                else:
                    writer.addfile(member)
    finally:
        plain.unlink()
    return True


def overlay_whiteouts(source, destination):
    """Rewrite one OCI layer tar with overlayfs whiteouts: ``.wh.x`` becomes
    a 0/0 character device ``x`` and ``.wh..wh..opq`` an opaque xattr on its
    directory, so per-layer bootstraps stack under OverlayFS."""
    with tarfile.open(source, "r:*") as reader:
        members = reader.getmembers()
        opaque = {member.name.rsplit("/", 1)[0] if "/" in member.name else "."
                  for member in members if member.name.rsplit("/", 1)[-1] == _OPAQUE}
        named = {member.name.rstrip("/") for member in members if member.isdir()}
        with tarfile.open(destination, "w", format=tarfile.PAX_FORMAT) as writer:
            for directory in sorted(opaque - named):  # An opaque directory the layer only implies.
                info = tarfile.TarInfo(directory)
                info.type, info.mode, info.pax_headers = tarfile.DIRTYPE, 0o755, {_OVERLAY_OPAQUE: "y"}
                writer.addfile(info)
            for member in members:
                parent, _, base = member.name.rpartition("/")
                if base == _OPAQUE:
                    continue
                if base.startswith(".wh."):
                    info = tarfile.TarInfo((parent + "/" if parent else "") + base[4:])
                    info.type, info.devmajor, info.devminor, info.mtime = tarfile.CHRTYPE, 0, 0, member.mtime
                    writer.addfile(info)
                    continue
                if member.isdir() and member.name.rstrip("/") in opaque:
                    member.pax_headers = {**member.pax_headers, _OVERLAY_OPAQUE: "y"}
                writer.addfile(member, reader.extractfile(member) if member.isfile() else None)


@dataclass
class LayerResult:
    diff_id: str
    bootstrap: bytes
    blob_id: str  # "" for a layer without file data.
    reused: bool = False


@dataclass
class RafsConverter:
    """Builder side of the chunk store; ``index`` holds the write token."""
    registry: object  # EnvironmentArtifactRegistry
    store: object  # ChunkObjectStore
    index: object  # ChunkIndexClient
    signing_key: object
    work_root: Path
    nydus_image: str = "nydus-image"
    layout: str = "image"
    verifier: object = None  # (images, layer tars) -> None; raises on any difference
    owner: str = field(default_factory=lambda: f"{socket.gethostname()}:{os.getpid()}")
    step: object = None  # Crash injection: called with each name in STEPS.
    metrics: dict = field(default_factory=dict)

    def _step(self, name, **details):
        if self.step is not None:
            self.step(name, **details)

    def _count(self, name, value=1):
        self.metrics[name] = self.metrics.get(name, 0) + value

    def convert(self, repository, reference, *, attach_tag=None):
        """Convert one image already in our registry; returns its root digest.

        ``attach_tag`` also tags a copy of the image manifest annotated with
        the new root (replacing any old one), so workers can run it before
        M2's dispatched roots; the source tag is never rewritten.
        """
        result = self._convert(repository, reference)
        if attach_tag is not None:
            document, _ = self.registry.client.manifest_document(repository, reference)
            if document.get("config", {}).get("digest") != result["source_image"]:
                raise ValueError("the image changed during conversion")
            annotated = strip_environment_annotation(document)
            annotated["annotations"] = {**annotated.get("annotations", {}), ENVIRONMENT_ANNOTATION: result["root"]}
            payload = canonical_bytes(annotated)
            self.registry.client.put_manifest(repository, attach_tag, payload, media_type=document["mediaType"])
            result["image_manifest"] = content_digest(payload)
        return result

    def _convert(self, repository, reference):
        if self.layout not in ("image", "layer"):
            raise ValueError("unknown RAFS mount granularity")
        client = self.registry.client
        document, _ = client.manifest_document(repository, reference)
        descriptor, layers = document.get("config"), document.get("layers")
        if (document.get("mediaType") not in (OCI_IMAGE, DOCKER_MANIFEST) or not isinstance(descriptor, dict)
                or not isinstance(layers, list) or type(descriptor.get("size")) is not int):
            raise ValueError("chunk-store conversion needs one OCI or Docker image manifest")
        config_digest = require_digest(descriptor.get("digest"))
        payload = client.blob_bytes(repository, config_digest, max_bytes=descriptor["size"])
        if content_digest(payload) != config_digest:
            raise ValueError("image config identity mismatch")
        config = json.loads(payload)
        diff_ids = list(config.get("rootfs", {}).get("diff_ids") or ())
        if len(diff_ids) != len(layers) or not diff_ids:
            raise ValueError("image config and manifest list different layers")
        self.work_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        with TemporaryDirectory(dir=self.work_root) as temporary:
            scratch = Path(temporary)
            results, tars = [], []
            for layer, diff_id in zip(layers, diff_ids):
                if diff_id == EMPTY_LAYER_DIFF_ID:
                    continue
                result, tar = self._layer(repository, layer, require_digest(diff_id), scratch)
                results.append(result)
                tars.append(tar)
            if not results or (self.layout == "layer" and len(results) > MAX_LAYERS_PER_ROOT):
                raise ValueError("image has no layers this granularity can mount")
            if self.layout == "image":
                images = [(self._merge(results, scratch), config_digest, [result.diff_id for result in results])]
                self._step("image_merged")
            else:
                images = [(result.bootstrap, None, [result.diff_id]) for result in results]
            signed = [self._component(bootstrap, source, sources) for bootstrap, source, sources in images]
            if self.verifier is not None:
                self.verifier(signed, tars)
            self._step("verified")
            return self._publish(signed, config_digest, config.get("config") or {}, diff_ids)

    def _layer(self, repository, descriptor, diff_id, scratch):
        """Claim, convert, pack and commit one layer, or reuse its bootstrap."""
        converter = converter_identity(self.layout)
        while True:
            claim = self.index.claim_layer(diff_id, converter, self.owner)
            if claim["state"] != "busy":
                break
            time.sleep(min(30, max(1, claim.get("retry_after", 1))))  # Another builder converts it.
        need_tar = self.verifier is not None or claim["state"] != "complete"
        tar = self._download(repository, descriptor, diff_id, scratch) if need_tar else None
        if claim["state"] == "complete":
            key = bootstrap_key(self.store.prefix, require_digest(claim["bootstrap"])[7:])
            compressed = self.store.get(key, MAX_BOOTSTRAP_BYTES)
            bootstrap = zstd_decompress(compressed, zstd_content_size(compressed, MAX_BOOTSTRAP_BYTES))
            if content_digest(bootstrap) != claim["bootstrap"]:
                raise ValueError("cached layer bootstrap identity mismatch")
            parsed = parse_bootstrap(bootstrap)
            self._count("layers_reused")
            return LayerResult(diff_id, bootstrap, parsed.devices[0][0] if parsed.devices else "", True), tar
        work = scratch / diff_id[7:19]
        (work / "blobs").mkdir(parents=True)
        layer, gzipped = tar, descriptor.get("mediaType", "").endswith(("gzip", "tar.gzip"))
        if path_ordered_layer(tar, work / "ordered.tar"):  # The verifier still reads the original.
            layer, gzipped = work / "ordered.tar", False
        if self.layout == "layer":
            source, kind = work / "overlay.tar", ["-t", "tar-rafs", "--whiteout-spec", "none"]
            overlay_whiteouts(layer, source)
        else:
            source, kind = layer, ["-t", "targz-rafs" if gzipped else "tar-rafs"]
        output = work / "layer.json"
        subprocess.run([self.nydus_image, "create", *kind, "--fs-version", "6", "--digester", "sha256",
                        "--compressor", "zstd", "--chunk-size", "0x40000",
                        "-D", str(work / "blobs"), "-B", str(work / "layer.boot"), "-J", str(output), str(source)],
                       check=True, capture_output=True, timeout=3600)
        blobs = json.loads(output.read_text())["blobs"]
        bootstrap = (work / "layer.boot").read_bytes()
        parsed = parse_bootstrap(bootstrap)
        if len(blobs) > 1 or [device[0] for device in parsed.devices] != blobs:
            raise ValueError("nydus-image produced an unexpected blob table for one layer")
        self._step("layer_converted", diff_id=diff_id)
        if blobs:
            self._pack(parsed, work / "blobs" / blobs[0], work, diff_id)
        bootstrap_digest = content_digest(bootstrap)
        self.store.put_bytes(bootstrap_key(self.store.prefix, bootstrap_digest[7:]), zstd_compress(bootstrap))
        self._step("layer_bootstrap_put", diff_id=diff_id)
        self.index.commit([], {"diff_id": diff_id, "converter": converter, "bootstrap": bootstrap_digest})
        self._step("layer_committed", diff_id=diff_id)
        self._count("layers_converted")
        return LayerResult(diff_id, bootstrap, blobs[0] if blobs else ""), tar

    def _download(self, repository, descriptor, diff_id, scratch):
        """The layer from our registry (no upstream pulls), digest and diff_id checked."""
        media = descriptor.get("mediaType", "")
        if "zstd" in media:
            raise ValueError("zstd OCI layers are not supported by the M1 converter")
        target = scratch / (require_digest(descriptor.get("digest"))[7:] + ".layer")
        if target.exists():
            return target
        digest = hashlib.sha256()
        with self.registry.client.open_blob(repository, descriptor["digest"]) as response, \
                open(str(target) + ".part", "wb") as stream:
            while chunk := response.read(1 << 20):
                digest.update(chunk)
                stream.write(chunk)
        if "sha256:" + digest.hexdigest() != descriptor["digest"]:
            raise ValueError("layer blob identity mismatch")
        diff = hashlib.sha256()
        with (gzip.open if media.endswith(("gzip", "tar.gzip")) else open)(str(target) + ".part", "rb") as stream:
            while chunk := stream.read(1 << 20):
                diff.update(chunk)
        if "sha256:" + diff.hexdigest() != diff_id:
            raise ValueError("layer diff_id does not match the image config")
        os.replace(str(target) + ".part", target)
        return target

    def _pack(self, bootstrap, blob, work, diff_id):
        """Verify, pack, upload and commit the chunks the index does not know,
        in blob order, one pack at a time.

        Converters running at once share chunks across different layers. The
        index is asked again before each batch and each pack is committed as
        soon as it is durable, so a chunk another converter committed meanwhile
        is skipped: the window is one pack, not one layer (M1 gate: 5.1 GB of
        duplicate chunks in 22.9 GB with whole-layer commits).
        """
        first = {}
        for chunk in sorted(bootstrap.chunks, key=lambda chunk: chunk[5]):
            first.setdefault(chunk[0], chunk)
        ids, count, writer = list(first), 0, None

        def flush(writer):
            digest, size = writer.finish()
            self.store.put_file(pack_key(self.store.prefix, digest), writer.path, size)
            self._count("pack_bytes", size)
            self._step("pack_put", diff_id=diff_id, pack=digest)
            # A pack is durable in S3 before any index row names it.
            self.index.commit([{"digest": digest, "size": size}])
            self._step("pack_committed", diff_id=diff_id, pack=digest)

        start = 0
        with open(blob, "rb") as source:
            while start < len(ids):
                batch = ids[start:start + LOOKUP_BATCH]
                following = start + len(batch)
                for offset, (chunk_id, known) in enumerate(zip(batch, self.index.lookup(batch))):
                    if known:
                        continue
                    _, _, flags, csize, usize, coff, _ = first[chunk_id]
                    payload = os.pread(source.fileno(), csize, coff)
                    stored, encoding = store_encoding(payload, usize, flags)
                    decode_chunk(stored, usize, encoding, chunk_id)  # Every new chunk decodes to its id.
                    if writer is not None and not writer.fits(len(stored)):
                        flush(writer)
                        writer, following = None, start + offset  # An upload passed: ask again from here.
                        break
                    if writer is None:
                        writer = PackWriter(work / f"pack-{count}")
                        count += 1
                    writer.add(chunk_id, stored, usize, encoding)
                    self._count("chunks_new")
                    self._count("chunk_bytes_new", len(stored))
                start = following
        if writer is not None:
            flush(writer)

    def _merge(self, results, scratch):
        paths = []
        for position, result in enumerate(results):
            path = scratch / f"merge-{position}.boot"
            path.write_bytes(result.bootstrap)
            paths.append(str(path))
        output = scratch / "image.boot"
        # --original-blob-ids, or merge names blobs after bootstrap files (S10).
        subprocess.run([self.nydus_image, "merge", "--original-blob-ids", ",".join(r.blob_id for r in results),
                        "-B", str(output), "-J", str(scratch / "image.json"), *paths],
                       check=True, capture_output=True, timeout=600)
        return output.read_bytes()

    def _component(self, bootstrap, source_image, source_layers):
        parsed = parse_bootstrap(bootstrap)
        chunk_map = chunk_map_from_bootstrap(parsed)
        encoded = chunk_map.encode()
        # Every id is committed and live; the reply locates this conversion's
        # packs and the store's for the verification mount.
        locator = self.index.locate(list(chunk_map.ids)) if chunk_map.ids else Locator(0, (), (), {})
        bootstrap_digest, map_digest = content_digest(bootstrap), content_digest(encoded)
        self.store.put_bytes(bootstrap_key(self.store.prefix, bootstrap_digest[7:]), zstd_compress(bootstrap))
        self.store.put_bytes(chunk_map_key(self.store.prefix, map_digest[7:]), encoded)
        self._step("metadata_put")
        component = sign_rafs_component(
            source_image=source_image, source_layers=source_layers,
            bootstrap={"digest": bootstrap_digest, "size": len(bootstrap)},
            chunk_map={"digest": map_digest, "size": len(encoded)}, device_size=chunk_map.device_size,
            layout=self.layout, signing_key=self.signing_key)
        self._step("component_signed")
        return component, bootstrap, chunk_map, locator

    def _publish(self, signed, config_digest, process, diff_ids):
        from .environment_manifest import EnvironmentManifest
        digests = []
        for component, *_ in signed:
            # One tag per component: images with equal layers but another
            # config share the chunk map, never the component.
            tag = "rafs-" + hashlib.sha256(canonical_bytes(component.to_dict())).hexdigest()
            digest = self.registry.publish_rafs(component, tag=tag)
            self._step("component_published")
            self.index.register(digest, component.bootstrap["digest"], component.chunk_map)
            self._step("registered")
            digests.append(digest)
        image_config = {"Entrypoint": process.get("Entrypoint") or [], "Cmd": process.get("Cmd") or [],
                        "Env": process.get("Env") or [], "WorkingDir": process.get("WorkingDir") or "",
                        "User": process.get("User") or ""}
        tag = "rafs-root-" + hashlib.sha256(canonical_bytes([config_digest, digests])).hexdigest()
        root = publish_environment(self.registry, source_image=config_digest,
                                   environment=EnvironmentManifest(digests[0], toolkits=tuple(digests[1:])),
                                   image_config=image_config, signing_key=self.signing_key, tag=tag,
                                   source_diff_ids=diff_ids)
        self._step("root_published")
        return {"root": root, "components": digests, "source_image": config_digest, "metrics": dict(self.metrics)}


# --- Verification: the converted tree against the OCI layers (§3 step 7) ---

def _normal(name):
    name = name[2:] if name.startswith("./") else name
    name = name.strip("/")
    return "" if name == "." else name


def expected_tree(layer_tars):
    """The final tree OCI semantics give: path -> attributes, hardlink groups
    (numbered: a layer replacing one member, as conda's pyc rewrites do, drops only it)."""
    tree, labels = {}, iter(range(1 << 62))
    for path in layer_tars:
        with tarfile.open(path, "r:*") as reader:
            members = reader.getmembers()
            # A layer's whiteouts and opaque markers hide only lower layers.
            for member in members:
                parent, _, base = _normal(member.name).rpartition("/")
                prefix = parent + "/" if parent else ""
                if base == _OPAQUE:
                    for existing in [key for key in tree if key.startswith(prefix) and key != parent]:
                        del tree[existing]
                elif base.startswith(".wh."):
                    victim = prefix + base[4:]
                    for existing in [key for key in tree if key == victim or key.startswith(victim + "/")]:
                        del tree[existing]
            for member in members:
                name = _normal(member.name)
                base = name.rpartition("/")[2]
                if not name or base.startswith(".wh."):
                    continue
                if member.islnk():
                    target = _normal(member.linkname)
                    if target in tree:
                        if tree[target][-1] is None:
                            tree[target] = tree[target][:-1] + (next(labels),)
                        tree[name] = tree[target]
                    continue
                for existing in [key for key in tree if key.startswith(name + "/")] if not member.isdir() else ():
                    del tree[existing]
                content = hashlib.sha256(reader.extractfile(member).read()).hexdigest() if member.isfile() else (
                    member.linkname if member.issym() else None)
                xattrs = tuple(sorted((key[len("SCHILY.xattr."):], value) for key, value in member.pax_headers.items()
                                      if key.startswith("SCHILY.xattr.") and not key.startswith(
                                          "SCHILY.xattr.trusted.overlay.")))
                kind = "dir" if member.isdir() else "symlink" if member.issym() else "file" if member.isfile() else (
                    "char" if member.ischr() else "block" if member.isblk() else "fifo")
                # Symlink permission bits are not portable (Linux reports 0777).
                tree[name] = (kind, None if kind == "symlink" else member.mode & 0o7777, member.uid, member.gid,
                              member.size if member.isfile() else 0, content,
                              None if member.isdir() else int(member.mtime), xattrs, None)
    return tree


def scan_tree(root):
    """The same attributes read from a mounted tree."""
    tree, inodes = {}, {}
    root = Path(root)
    for directory, names, files in os.walk(root):
        for name in names + files:
            path = Path(directory) / name
            info = path.lstat()
            relative = str(path.relative_to(root))
            mode = info.st_mode
            kind = ("dir" if stat.S_ISDIR(mode) else "symlink" if stat.S_ISLNK(mode) else "file" if stat.S_ISREG(mode)
                    else "char" if stat.S_ISCHR(mode) else "block" if stat.S_ISBLK(mode) else "fifo")
            content = None
            if kind == "file":
                with open(path, "rb") as stream:
                    content = hashlib.file_digest(stream, "sha256").hexdigest() if hasattr(hashlib, "file_digest") \
                        else hashlib.sha256(stream.read()).hexdigest()
            elif kind == "symlink":
                content = os.readlink(path)
            try:
                names_ = os.listxattr(path, follow_symlinks=False)
            except OSError:
                names_ = []
            xattrs = tuple(sorted((key, os.getxattr(path, key, follow_symlinks=False).decode(errors="surrogateescape"))
                                  for key in names_ if not key.startswith("trusted.overlay.")))
            group = None
            if kind == "file" and info.st_nlink > 1:
                group = inodes.setdefault((info.st_dev, info.st_ino), relative)
            tree[relative] = (kind, None if kind == "symlink" else stat.S_IMODE(mode), info.st_uid, info.st_gid,
                              info.st_size if kind == "file" else 0, content,
                              None if kind == "dir" else int(info.st_mtime), xattrs, group)
    return tree


def compare_trees(expected, actual, *, limit=20):
    """Differences, as text; directory times are not compared (layout 2)."""
    def groups(tree):
        found = {}
        for name, attributes in tree.items():
            if attributes[-1] is not None:
                found.setdefault(attributes[-1], set()).add(name)
        return sorted(sorted(group) for group in found.values() if len(group) > 1)
    differences = []
    for name in sorted(set(expected) | set(actual)):
        if name not in actual or name not in expected:
            differences.append(f"{name}: {'missing' if name not in actual else 'unexpected'}")
        elif expected[name][:-1] != actual[name][:-1]:
            differences.append(f"{name}: expected {expected[name][:-1]}, found {actual[name][:-1]}")
        if len(differences) >= limit:
            return differences
    wanted, found = groups(expected), groups(actual)
    if wanted != found:
        differences.append("hardlink groups differ: " + "; ".join(",".join(group[:4]) for group in (
            [group for group in wanted if group not in found] + [group for group in found if group not in wanted])[:3]))
    return differences


@contextmanager
def mounted_images(images, root, *, devices, trusted_keys, cache=None, runner=subprocess.run):
    """Mount verified RAFS images through the worker's own device (root, NBD
    and kernel EROFS with device tables, Linux 5.16+); yields the tree."""
    from .environment_cache import VerifiedEnvironmentCache
    from .environment_nbd import EnvironmentReadWorkers, ReadOnlyEnvironmentDevice
    cache = cache or VerifiedEnvironmentCache(Path(root) / "cache", None, max_bytes=8 * 1024 ** 3,
                                              concurrent_misses=32)
    workers, attached, mounts = EnvironmentReadWorkers(), [], []
    try:
        free = iter(devices)
        for position, image in enumerate(images):
            for device in free:
                try:
                    attached.append(ReadOnlyEnvironmentDevice(Path(device), image, cache, workers,
                                                              trusted_keys=trusted_keys))
                    break
                except OSError:
                    continue
            else:
                raise RuntimeError("no free NBD device for verification")
            target = Path(root) / f"lower-{position}"
            target.mkdir(parents=True, exist_ok=True)
            runner(["mount", "-t", "erofs", "-o", "ro", str(attached[-1].path), str(target)], check=True)
            mounts.append(target)
        if len(mounts) == 1:
            yield mounts[0]
        else:
            merged = Path(root) / "merged"
            merged.mkdir(exist_ok=True)
            runner(["mount", "-t", "overlay", "overlay", "-o",
                    "ro,lowerdir=" + ":".join(str(path) for path in reversed(mounts)), str(merged)], check=True)
            mounts.append(merged)
            yield merged
    finally:
        for target in reversed(mounts):
            runner(["umount", str(target)], check=False)
        for device in attached:
            device.close()
        workers.close()
        cache.close()


def mount_verifier(*, devices, trusted_keys, work_root, store_node=None):
    """§3 step 7: mount through the worker's own RAFS device and compare the
    whole tree with the OCI layers before anything is published. ``store_node``
    (URL, prefix, token) reads through the node, as workers do: S3's tail
    outlasts the kernel's 30 s NBD timeout under load (S12, M1 gate)."""
    def verify(signed, tars):
        from .environment_rafs import RafsImage, store_access, store_locator
        options = {}
        if store_node is not None:
            from .chunk_store_node import ChunkStoreClient, locator_objects
            base_url, prefix, token = store_node
            options = {"reader": store_access(base_url, token)[0], "origin": base_url}
            signed = [(component, bootstrap, chunk_map, store_locator(locator, base_url, prefix))
                      for component, bootstrap, chunk_map, locator in signed]
            # Warm first: a fill retries past S3's tail (p99 12 s, max 58 s with
            # 12 converters) where a mounted read would hit the NBD timeout.
            objects = {item["key"]: item for *_, locator in signed for item in locator_objects(locator, base_url)}
            client = ChunkStoreClient(base_url, token)
            if objects:
                client.wait(client.warm(list(objects.values()))["job"], timeout=1800)
        images = [RafsImage(None, component, bootstrap, chunk_map, locator, **options)
                  for component, bootstrap, chunk_map, locator in signed]
        with TemporaryDirectory(dir=work_root) as temporary, \
                mounted_images(images, temporary, devices=devices, trusted_keys=trusted_keys) as tree:
            differences = compare_trees(expected_tree(tars), scan_tree(tree))
        if differences:
            raise ValueError("converted tree differs from the OCI layers: " + "; ".join(differences))
    return verify


# --- Rollback: chunk store -> tar -> OCI push (design §7) ---

def exact_symlinks(source, destination, targets):
    """``source`` with each symlink's target as the image has it (``targets``,
    by path): ``nydus-image unpack``'s tar writer drops '.' components
    (``.././61/adm1178`` becomes ``../61/adm1178``; M1 gate rollback). Returns
    ``source`` when nothing differs."""
    with tarfile.open(source, "r:") as reader:
        members = reader.getmembers()
        if all(not member.issym() or targets.get(_normal(member.name), member.linkname) == member.linkname
               for member in members):
            return source
        with tarfile.open(destination, "w", format=tarfile.PAX_FORMAT) as writer:
            for member in members:
                if member.issym():
                    member.linkname = targets.get(_normal(member.name), member.linkname)
                writer.addfile(member, reader.extractfile(member) if member.isfile() else None)
    return destination


def unpack_environment(registry, index, root, *, repository, tag, work_root, nydus_image="nydus-image",
                       reader=None, access=None):
    """Regenerate a single-layer OCI image from a converted root.

    Supports ``image`` layout roots. The blobs are rebuilt uncompressed from
    verified chunks and a private copy of the bootstrap's chunk table is
    rewritten to match, so ``nydus-image unpack`` needs no original blob.
    """
    from .chunk_index import http_range
    from .environment_builder import _gzip_layer
    from .environment_rafs import load_rafs_image
    environment = load_environment(registry, root)
    base = registry.load(environment.environment.base)
    if not isinstance(base, RafsEnvironmentComponent) or base.format["layout"] != "image":
        raise ValueError("unpack supports roots with one image-layout RAFS base")
    image = load_rafs_image(environment.environment.base, base, index, **(access or {"reader": reader or http_range}))
    with TemporaryDirectory(dir=work_root) as temporary:
        scratch = Path(temporary)
        blobs = scratch / "blobs"
        blobs.mkdir()
        _rebuild_blobs(image, blobs)
        bootstrap = image.bootstrap.read(0, image.bootstrap.size)
        patched = scratch / "patched.boot"
        patched.write_bytes(_uncompressed_chunk_table(bootstrap))
        subprocess.run([nydus_image, "unpack", "--bootstrap", str(patched), "--blob-dir", str(blobs),
                        "--output", str(scratch / "layer.tar")], check=True, capture_output=True, timeout=3600)
        layer = exact_symlinks(scratch / "layer.tar", scratch / "exact.tar", symlink_targets(bootstrap))
        diff_id, layer_digest, layer_size = _gzip_layer(layer, scratch / "layer.tar.gz")
        client = registry.client
        if not client.blob_exists(repository, layer_digest):
            client.upload_blob_file(repository, scratch / "layer.tar.gz", layer_digest, layer_size)
    config = canonical_bytes({"architecture": "amd64", "os": "linux", "config": environment.image_config,
                              "rootfs": {"type": "layers", "diff_ids": [diff_id]},
                              "history": [{"created_by": "ucloud-sandboxes unpack-environment", "comment": root}]})
    from .environment_artifact import _upload_blob
    _upload_blob(client, repository, config, content_digest(config))
    manifest = canonical_bytes(strip_environment_annotation({
        "schemaVersion": 2, "mediaType": OCI_IMAGE,
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": content_digest(config),
                   "size": len(config)},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": layer_digest,
                    "size": layer_size}]}))
    client.put_manifest(repository, tag, manifest, media_type=OCI_IMAGE)
    return {"manifest_digest": content_digest(manifest), "diff_id": diff_id, "root": root,
            "toolkits": list(environment.environment.toolkits)}


def _rebuild_blobs(image, directory, range_bytes=8 * 1024 ** 2):
    """Each blob's uncompressed bytes (its device region), from verified chunks."""
    files = {}
    try:
        for blob, mapped, blocks in image.map.regions:
            files[blob] = (open(directory / blob, "wb"), mapped * BLOCK)
            files[blob][0].truncate(blocks * BLOCK)
        targets = {}  # Chunk digest -> (file, offset) of every place it sits.
        for index, chunk in enumerate(image.chunks):
            offset = image.map.offsets[index]
            for blob, mapped, blocks in image.map.regions:
                if mapped * BLOCK <= offset < (mapped + blocks) * BLOCK:
                    targets.setdefault(chunk.digest, []).append((files[blob][0], offset - mapped * BLOCK))
        queue = list(reversed(image.prefetch_order(range(len(image.chunks)))))
        while queue:
            run, _ = image.next_run(queue, lambda chunk: False, limit=range_bytes)
            verified = image.fetch_run(run, 120.0, limit=range_bytes)
            for index in run:
                data = verified.get(image.chunks[index].digest)
                if data is None:
                    raise ValueError("a chunk did not verify while rebuilding blobs")
                for stream, offset in targets[image.chunks[index].digest]:
                    os.pwrite(stream.fileno(), data, offset)
    finally:
        for stream, _ in files.values():
            stream.close()


def _uncompressed_chunk_table(bootstrap):
    """A private copy whose chunk records name raw data at their own offsets."""
    import struct
    data = bytearray(bootstrap)
    record = struct.Struct("<32sIIIIQQQII")
    _, _, _, _, offset, size = struct.unpack_from("<QQIIQQ", data, 1024 + 128)
    for position in range(offset, offset + size, record.size):
        digest, blob, flags, _, usize, _, uoff, foff, index, crc = record.unpack_from(data, position)
        record.pack_into(data, position, digest, blob, flags & ~1, usize, usize, uoff, uoff, foff, index, crc)
    return bytes(data)


# --- Commands: serve-chunk-index, convert-environment, unpack-environment ---

def _chunk_store(path):
    from .config import DeploymentConfig
    selected = DeploymentConfig.from_file(Path(path)).immutable_environments
    return selected.chunk_store if selected is not None else None


def serve_chunk_index(args):
    """``ucloud-chunk-index`` on the gateway (``--config``) or, with
    ``store_node.serve_index``, on the store node (``--chunk-store-config``);
    exits 78 where it is not configured to run."""
    from .chunk_index import ChunkIndex, ChunkIndexServer, ChunkIndexService
    from .environment_config import ChunkStoreConfig, read_token
    on_node = args.chunk_store_config is not None
    store = ChunkStoreConfig.from_file(args.chunk_store_config) if on_node else _chunk_store(args.config)
    if store is None:
        print("immutable_environments.chunk_store is not configured", flush=True)
        return 78
    # The gateway's unit makes the tokens as the service user, even when the
    # index lives on the store node; store-node init copies them there.
    paths = (store.read_token_file, store.write_token_file)
    tokens = None if on_node else [read_token(path, create=True) for path in paths]
    if (store.store_node is not None and store.store_node.serve_index) != on_node:
        print("ucloud-chunk-index is configured to run on the other host", flush=True)
        return 78
    tokens = tokens or [read_token(path) for path in paths]
    Path(store.index_database).parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    service = ChunkIndexService(ChunkIndex(store.index_database), store.object_store(),
                                store_url=store.store_node.url if store.store_node else None)
    host, port = store.index_listen.rsplit(":", 1)
    with ChunkIndexServer((host, int(port)), service, read_token=tokens[0], write_token=tokens[1]) as server:
        server.serve_forever()
    return 0


def _registry_and_index(args, store):
    from .chunk_index import ChunkIndexClient
    from .environment_config import environment_registry_from_args, read_token
    registry = environment_registry_from_args(args)
    if registry is None or store is None:
        raise ValueError("chunk-store commands need environment registry trust and immutable_environments.chunk_store")
    return registry, ChunkIndexClient(store.index_url, read_token(args.chunk_index_token_file).decode())


def convert_command(args):
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from .environment_config import read_token
    from .managed_registry import manifest_digest_from_image_ref, registry_repository_tag_from_image_ref
    store = _chunk_store(args.config)
    registry, index = _registry_and_index(args, store)
    coordinates = registry_repository_tag_from_image_ref(args.image_ref)
    if coordinates is None:
        raise ValueError("convert-environment needs an image in the managed registry")
    key = load_pem_private_key(Path(args.environment_signing_key).read_bytes(), password=None)
    store_node = store.store_node and (store.store_node.url, store.prefix,
                                       read_token(args.chunk_index_token_file).decode())
    verifier = mount_verifier(devices=args.verify_device, trusted_keys=registry.trusted_keys,
                              work_root=args.work_root, store_node=store_node) if args.verify_device else None
    converter = RafsConverter(registry, store.object_store(), index, key, args.work_root,
                              nydus_image=store.nydus_image, layout=args.layout or store.mount_granularity,
                              verifier=verifier, **({"owner": args.owner} if args.owner else {}))
    result = converter.convert(coordinates[0], manifest_digest_from_image_ref(args.image_ref) or coordinates[1],
                               attach_tag=args.attach_tag or None)
    print(json.dumps(result, sort_keys=True))
    return 0


def unpack_command(args):
    from .managed_registry import registry_repository_tag_from_image_ref
    store = _chunk_store(args.config)
    registry, index = _registry_and_index(args, store)
    coordinates = registry_repository_tag_from_image_ref(args.output_ref)
    if coordinates is None or "@" in args.output_ref:
        raise ValueError("unpack-environment needs an owned output tag")
    args.work_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    access = None
    if store.store_node is not None:  # Locators name the store node: read there, with the token.
        from .environment_config import read_token
        from .environment_rafs import store_access
        reader, getter = store_access(store.store_node.url, read_token(args.chunk_index_token_file).decode())
        access = {"reader": reader, "getter": getter, "origin": store.store_node.url}
    result = unpack_environment(registry, index, require_digest(args.root), repository=coordinates[0],
                                tag=coordinates[1], work_root=args.work_root, nydus_image=store.nydus_image,
                                access=access)
    print(json.dumps(result, sort_keys=True))
    return 0


def add_commands(subparsers):
    from .environment_config import add_environment_registry_args
    serve = subparsers.add_parser("serve-chunk-index", help="Run the chunk store index (ucloud-chunk-index).")
    where = serve.add_mutually_exclusive_group(required=True)
    where.add_argument("--config", type=Path, help="the gateway's deployment.json")
    where.add_argument("--chunk-store-config", type=Path, help="the store node's chunk_store JSON")
    serve.set_defaults(func=serve_chunk_index)
    from .chunk_store_node import add_commands as add_node_commands
    add_node_commands(subparsers)
    for name, function, text in (
            ("convert-environment", convert_command, "Convert a registry image into the chunk store (RAFS v6)."),
            ("unpack-environment", unpack_command, "Regenerate an OCI image from a chunk-store root (rollback).")):
        command = subparsers.add_parser(name, help=text)
        command.add_argument("--config", type=Path, required=True, help="deployment.json with chunk_store")
        command.add_argument("--chunk-index-token-file", type=Path, required=True)
        command.add_argument("--work-root", type=Path, required=True)
        add_environment_registry_args(command)
        if name == "convert-environment":
            command.add_argument("--image-ref", required=True)
            command.add_argument("--environment-signing-key", type=Path, required=True)
            command.add_argument("--layout", choices=("image", "layer"))
            command.add_argument("--owner", default="")
            command.add_argument("--attach-tag", default="", help="also tag an annotated copy for workers")
            command.add_argument("--verify-device", action="append", default=[],
                                 help="NBD device for the full-tree verification mount (root); repeatable")
        else:
            command.add_argument("--root", required=True)
            command.add_argument("--output-ref", required=True)
        command.set_defaults(func=function)

