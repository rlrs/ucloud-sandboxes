"""Build immutable components from a fresh allowlisted view of OCI image input.

The only public input is the existing immutable Docker image adapter. No runtime
workspace, memory directory, or checkpoint is accepted as a publication source.
"""
from dataclasses import dataclass, field
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
import errno
import fcntl
import json
import logging
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import tarfile
import time
from tempfile import TemporaryDirectory

from .direct_warden import DirectWardenError
from .environment_artifact import (
    EMPTY_LAYER_DIFF_ID, LAYER_TAG_PREFIX, OCI_IMAGE, LayerEnvironmentComponent, canonical_bytes,
    MAX_INDEX_BYTES, content_digest, layer_chain_id, layer_group_key, require_digest,
    sign_component, sign_layer_component,
)
from .image_rootfs import DockerImageConfig, DockerOverlay2RootfsStore
from .managed_registry import RegistryRequestError
from .build_deadline import (
    ImageBuildTimeoutError, build_execution_deadline,
    remaining_build_execution_seconds, without_build_execution_deadline,
)

_LOG = logging.getLogger(__name__)
_PUBLICATION_METRICS = ContextVar("environment_publication_metrics", default=None)


@contextmanager
def publication_metrics():
    """Collect per-publication measurements without mixing concurrent builds."""
    current = _PUBLICATION_METRICS.get()
    if current is not None:
        yield current
        return
    values = {}
    token = _PUBLICATION_METRICS.set(values)
    try:
        yield values
    finally:
        _PUBLICATION_METRICS.reset(token)


def _measure(name, value=1):
    values = _PUBLICATION_METRICS.get()
    if values is not None:
        values[name] = values.get(name, 0) + value


@contextmanager
def _phase(name):
    started = time.monotonic()
    try:
        yield
    finally:
        _measure(name + "_ms", round((time.monotonic() - started) * 1000, 3))


def _copy_metadata(source, destination, info, *, skip_xattrs=()):
    # Ownership first: chown(2) clears security.capability and setuid bits,
    # so file capabilities and the mode are applied after it.
    os.chown(destination, info.st_uid, info.st_gid, follow_symlinks=False)
    if not stat.S_ISLNK(info.st_mode):
        os.chmod(destination, stat.S_IMODE(info.st_mode))
    if hasattr(os, "listxattr"):
        for name in os.listxattr(source, follow_symlinks=False):
            if name not in skip_xattrs:
                os.setxattr(destination, name, os.getxattr(source, name, follow_symlinks=False),
                            follow_symlinks=False)
    os.utime(destination, ns=(info.st_atime_ns, info.st_mtime_ns), follow_symlinks=False)


def _copy_entry(source, destination, hardlinks):
    remaining_build_execution_seconds()
    info = source.lstat()
    if stat.S_ISDIR(info.st_mode):
        destination.mkdir(mode=0o700)
        for child in sorted(source.iterdir(), key=lambda path: path.name):
            # This is a mounted immutable filesystem, not an OCI layer tar.
            # A literal .wh.* filename is ordinary user data. Real filesystem
            # whiteout devices and opaque xattrs retain their exact semantics.
            _copy_entry(child, destination / child.name, hardlinks)
    elif stat.S_ISREG(info.st_mode):
        identity = (info.st_dev, info.st_ino)
        previous = hardlinks.get(identity)
        if previous is not None:
            os.link(previous, destination)
        else:
            # Input is an immutable image lease; NOFOLLOW also fences a replaced
            # final path and the post-read stat rejects a changing builder view.
            descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "rb") as reader, destination.open("xb") as writer:
                current = os.fstat(reader.fileno())
                if (current.st_dev, current.st_ino) != identity:
                    raise ValueError("immutable build input changed")
                while chunk := reader.read(1024 * 1024):
                    remaining_build_execution_seconds()
                    writer.write(chunk)
                after = os.fstat(reader.fileno())
                if (after.st_size, after.st_mtime_ns) != (info.st_size, info.st_mtime_ns):
                    raise ValueError("immutable build input changed while copying")
            hardlinks[identity] = destination
    elif stat.S_ISLNK(info.st_mode):
        destination.symlink_to(os.readlink(source))
    elif stat.S_ISCHR(info.st_mode) and info.st_rdev == os.makedev(0, 0):
        os.mknod(destination, stat.S_IFCHR | 0o600, info.st_rdev)
    else:
        raise ValueError("environment build view contains an unsupported special file")
    _copy_metadata(source, destination, info)


# Mounted by the sandbox runtime; Docker images carry them empty.
WHOLE_IMAGE_EXCLUDED = frozenset({"dev", "proc", "sys", "run"})


def expand_allowlist(source_root: Path, paths) -> list:
    """Expand ``*`` to every top-level image entry except runtime mounts.

    Arbitrary imported and task images keep content in places a fixed list
    cannot know (``/testbed``, ``/app``, ``/opt/conda``), and a listed path
    that an image lacks is an error.
    """
    expanded = []
    for value in paths:
        if value == "*":
            expanded.extend(sorted(
                entry.name for entry in source_root.iterdir()
                if entry.name not in WHOLE_IMAGE_EXCLUDED
            ))
        else:
            expanded.append(value)
    return expanded


def _whole_image(allowlist) -> bool:
    values = list(allowlist)
    return bool(values) and all(value == "*" for value in values)


def allowlisted_build_view(source_root: Path, destination: Path, paths):
    """Copy declared immutable image paths, preserving merged-layer semantics."""
    if source_root.is_symlink() or not source_root.is_dir() or destination.exists():
        raise ValueError("fresh environment view requires a real source and new destination")
    allowed = []
    for value in expand_allowlist(source_root, paths):
        if not isinstance(value, str):
            raise ValueError("environment allowlist paths must be strings")
        path = PurePosixPath(value)
        if (path.is_absolute() or not path.parts
                or any(part in {".", ".."} for part in path.parts) or "\0" in value):
            raise ValueError("environment allowlist paths must stay within the immutable image")
        if path not in allowed:
            allowed.append(path)
    if not allowed:
        raise ValueError("an explicit environment publication allowlist is required")
    allowed = sorted(path for path in allowed if not any(parent in allowed for parent in path.parents))
    destination.mkdir(mode=0o700)
    hardlinks = {}
    for path in allowed:
        source = source_root
        target = destination
        for part in path.parts[:-1]:
            source /= part
            target /= part
            if source.is_symlink() or not source.is_dir():
                raise ValueError("allowlist traverses a non-directory or symlink")
            target.mkdir(mode=0o700, exist_ok=True)
        source /= path.name
        if not source.exists() and not source.is_symlink():
            raise ValueError("allowlisted build path is absent")
        _copy_entry(source, target / path.name, hardlinks)
    # Parents synthesized for narrow allowlist entries also retain source
    # ownership/mode/xattrs; do this last so readonly directories remain writable
    # while the fresh view is being assembled.
    for path in sorted(destination.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        original = source_root / path.relative_to(destination)
        if path.is_dir() and not path.is_symlink() and original.is_dir():
            _copy_metadata(original, path, original.lstat())
    _copy_metadata(source_root, destination, source_root.lstat())


# Per-layer publication. Docker overlay2 diff directories encode deletions as
# overlayfs does: a 0:0 character device whiteout and an opaque-directory
# xattr. A group of layers becomes one component; the worker stacks the
# components with overlayfs exactly as Docker stacks the layers.
OPAQUE_XATTR = "trusted.overlay.opaque"
# A group closes once its compressed layers reach this size, and a layer at
# least this large is a group of its own: a shared base becomes a few large,
# stable components and each task's small layers one more.
LAYER_GROUP_BYTES = 64 * 1024 ** 2
# The worker's overlay stacks every component below one mount; the manifest
# allows 33 components and the mount option one page.
MAX_LAYER_GROUPS = 24


def plan_layer_groups(sizes, *, threshold=LAYER_GROUP_BYTES, max_groups=MAX_LAYER_GROUPS):
    """Half-open layer index ranges, bottom to top.

    Greedy from the bottom, so a group depends only on the layers at and below
    it: images sharing a base plan identical groups for it. The layers above
    the last closed group form one group. Beyond ``max_groups`` the top groups
    merge, keeping every lower group unchanged.
    """
    if max_groups < 1:
        raise ValueError("layer planning requires at least one group")
    groups, start, total = [], 0, 0
    for index, size in enumerate(sizes):
        if type(size) is not int or size < 0:
            raise ValueError("invalid layer size")
        if size >= threshold and index > start:
            groups.append((start, index))
            start, total = index, 0
        total += size
        if total >= threshold:
            groups.append((start, index + 1))
            start, total = index + 1, 0
    if start < len(sizes):
        groups.append((start, len(sizes)))
    if len(groups) > max_groups:
        groups = groups[:max_groups - 1] + [(groups[max_groups - 1][0], len(sizes))]
    return groups


def _is_whiteout(info):
    return stat.S_ISCHR(info.st_mode) and info.st_rdev == os.makedev(0, 0)


def _is_opaque(path):
    if not hasattr(os, "getxattr"):
        return False
    try:
        return os.getxattr(path, OPAQUE_XATTR, follow_symlinks=False) == b"y"
    except OSError:
        return False


def _make_whiteout(path):
    os.mknod(path, stat.S_IFCHR | 0o600, os.makedev(0, 0))


def _set_opaque(path):
    os.setxattr(path, OPAQUE_XATTR, b"y", follow_symlinks=False)


def _lstat(path):
    try:
        return path.lstat()
    except FileNotFoundError:
        return None


def _remove(path, info):
    if stat.S_ISDIR(info.st_mode):
        shutil.rmtree(path)
    else:
        path.unlink()


class _LowerView:
    """Overlay lookup through Docker diff directories, top to bottom."""

    def __init__(self, layers):
        self.layers = tuple(layers)

    def exists(self, parts):
        directories = list(self.layers)
        for depth, part in enumerate(parts):
            found = []
            for directory in directories:
                candidate = directory / part
                try:
                    info = candidate.lstat()
                except (FileNotFoundError, NotADirectoryError):
                    continue
                if _is_whiteout(info):
                    break
                if not stat.S_ISDIR(info.st_mode):
                    # A non-directory ends the lookup; below a directory it
                    # only stops the merge.
                    if not found and depth == len(parts) - 1:
                        return True
                    break
                found.append(candidate)
                if _is_opaque(candidate):
                    break
            if not found:
                return False
            directories = found
        return True


def _replace_directory_metadata(source, destination, info=None):
    """A merged directory shows the upper layer's inode; its opacity stays."""
    if info is None:
        info = source.lstat()
    if hasattr(os, "listxattr"):
        wanted = set(os.listxattr(source, follow_symlinks=False)) - {OPAQUE_XATTR}
        for name in os.listxattr(destination, follow_symlinks=False):
            if name != OPAQUE_XATTR and name not in wanted:
                os.removexattr(destination, name, follow_symlinks=False)
    _copy_metadata(source, destination, info, skip_xattrs=(OPAQUE_XATTR,))


def _merge_layer_diff(source, target, parts, hardlinks, lower, lower_visible, *, consume_private_diffs=False):
    for child in sorted(source.iterdir(), key=lambda path: path.name):
        remaining_build_execution_seconds()
        if not parts and child.name in WHOLE_IMAGE_EXCLUDED:
            continue  # mkfs excludes them; they may hold device nodes.
        destination = target / child.name
        path = (*parts, child.name)
        info = child.lstat()
        existing = _lstat(destination)
        if _is_whiteout(info):
            if existing is not None:
                _remove(destination, existing)
            # Keep a whiteout only while it hides something beneath the group:
            # in a directory no lower layer has, overlayfs would list it.
            if lower_visible and lower.exists(path):
                _make_whiteout(destination)
            continue
        if not stat.S_ISDIR(info.st_mode):
            if existing is not None:
                _remove(destination, existing)
            if consume_private_diffs and (stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)):
                # Only freshly extracted, private scratch diffs opt in. Moving
                # their inode preserves bytes, ownership, xattrs and hardlinks
                # without copying or applying metadata a second time. Docker
                # layers and other borrowed immutable inputs always copy.
                try:
                    child.rename(destination)
                except OSError as exc:
                    if exc.errno != errno.EXDEV:
                        raise
                    _copy_entry(child, destination, hardlinks)
                else:
                    if stat.S_ISREG(info.st_mode):
                        hardlinks.setdefault((info.st_dev, info.st_ino), destination)
            else:
                _copy_entry(child, destination, hardlinks)
            continue
        opaque = _is_opaque(child)
        if existing is not None and stat.S_ISDIR(existing.st_mode) and not opaque:
            _merge_layer_diff(child, destination, path, hardlinks, lower,
                              lower_visible and not _is_opaque(destination),
                              consume_private_diffs=consume_private_diffs)
            _replace_directory_metadata(child, destination, info if consume_private_diffs else None)
            continue
        # A directory over an earlier file or whiteout of this group hides
        # every lower layer, as it does in the separate layers: make it opaque.
        hides = opaque or existing is not None
        if existing is not None:
            _remove(destination, existing)
        destination.mkdir(mode=0o700)
        _merge_layer_diff(child, destination, path, hardlinks, lower, lower_visible and not hides,
                          consume_private_diffs=consume_private_diffs)
        _copy_metadata(child, destination, info)
        if hides:
            _set_opaque(destination)


def squash_layer_diffs(diff_dirs, destination: Path, *, lower_dirs=(), consume_private_diffs=False):
    """Squash Docker overlay2 diffs (bottom to top) into one overlay layer.

    Stacked above ``lower_dirs`` (bottom to top) the result shows the same tree
    as the separate layers: a whiteout deletes its path and stays while a lower
    layer has that path; an opaque directory replaces the earlier one; a
    directory over an earlier file or whiteout becomes opaque; a file replaces
    whatever was there, breaking earlier hard links.

    consume_private_diffs may move regular files and symlinks out of privately
    owned disposable extractions. Never enable it for Docker or borrowed layer
    directories. Directory metadata and the overlay merge rules remain the
    same; cross-filesystem moves fall back to copying.
    """
    if destination.exists() or not diff_dirs:
        raise ValueError("layer squash requires layers and a new destination")
    destination.mkdir(mode=0o700)
    lower = _LowerView(tuple(reversed(tuple(lower_dirs))))
    for layer in diff_dirs:
        if layer.is_symlink() or not layer.is_dir():
            raise ValueError("layer squash requires real diff directories")
        info = layer.lstat() if consume_private_diffs else None
        # Hard links never span layers: each diff is its own tar extraction.
        _merge_layer_diff(layer, destination, (), {}, lower, bool(lower.layers),
                          consume_private_diffs=consume_private_diffs)
        _replace_directory_metadata(layer, destination, info)


@dataclass
class FreshEnvironmentBuilder:
    image_store: DockerOverlay2RootfsStore
    registry: object
    signing_key: object
    work_root: Path
    mkfs_erofs: str = "mkfs.erofs"
    # lz4 made SWE-bench images 38% smaller with a faster mkfs; workers read
    # fewer bytes per chunk and the kernel decompresses (docs/image-import.md).
    compression: str = "lz4"
    # Production builders isolate CPU-heavy private preparation from the GIL
    # shared by concurrent build threads. Test adapters retain the local path.
    preparation_subprocess: bool = False
    release_published_tag: bool = False
    _layer_format: dict | None = field(default=None, init=False, repr=False)

    def build(self, image_ref, *, allowlist, tag):
        if not isinstance(self.image_store, DockerOverlay2RootfsStore):
            raise ValueError("environment publication requires the immutable OCI build adapter")
        self.work_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        image_id = None
        try:
            with self.image_store.operation_lease(image_ref) as source:
                image_id = source.image_id
                with TemporaryDirectory(dir=self.work_root) as temporary:
                    root = Path(temporary)
                    image = root / "component.erofs"
                    if _whole_image(allowlist):
                        # The merged overlay already is the image; copying every
                        # file into a fresh view only repeated it (13 s for 2.6 GB,
                        # serialized on the GIL across concurrent publications).
                        if source.rootfs.is_symlink() or not source.rootfs.is_dir():
                            raise ValueError("environment publication requires a real source")
                        view, exclude = source.rootfs, True
                    else:
                        view, exclude = root / "view", False
                        allowlisted_build_view(source.rootfs, view, allowlist)
                    self._mkfs(image, view, exclude_runtime_mounts=exclude)
                    component = sign_component(image, source_image=source.image_id, signing_key=self.signing_key)
                    digest = self.registry.publish(image, component, tag=tag)
                    return {"image_id": source.image_id, "component_digest": digest,
                            "component": component, "image_config": source.image_config}
        finally:
            if image_id is not None:
                # Builders have no sandbox registry users of this temporary
                # mount. Existing image leases fence concurrent build readers.
                self._collect_image(image_id)

    def _collect_image(self, image_id):
        # Another publisher may still hold a lease on the same image. Cleanup
        # cannot extend an expired build indefinitely; ordinary image GC can
        # reclaim a cache entry whose readers have not drained in this budget.
        try:
            with without_build_execution_deadline(), build_execution_deadline(10):
                self.image_store.collect_image(image_id, is_referenced=lambda _: False)
        except (ImageBuildTimeoutError, subprocess.TimeoutExpired):
            _LOG.warning("temporary builder image cleanup deferred after its deadline")

    def _mkfs(self, image, view, *, exclude_runtime_mounts):
        options = ["-T", "0", "-U", "00000000-0000-0000-0000-000000000000"]
        if self.compression:
            options.append("-z" + self.compression)
        if exclude_runtime_mounts:
            options.append("--exclude-regex=^(" + "|".join(sorted(WHOLE_IMAGE_EXCLUDED)) + ")$")
        subprocess.run((self.mkfs_erofs, *options, str(image), str(view)),
                       check=True, capture_output=True, timeout=remaining_build_execution_seconds(600))

    def layer_format(self):
        """Everything besides the layers that decides a layer component's bytes."""
        if self._layer_format is None:
            result = subprocess.run((self.mkfs_erofs, "-V"), check=True, capture_output=True,
                                    text=True, timeout=remaining_build_execution_seconds(60))
            lines = (result.stdout.strip() or result.stderr.strip()).splitlines()
            if not lines:
                raise ValueError("mkfs.erofs reported no version")
            self._layer_format = {"layout": 1, "mkfs": lines[0].strip(), "compression": self.compression or "",
                                  "excludes": sorted(WHOLE_IMAGE_EXCLUDED)}
        return self._layer_format

    def build_layers(self, image_ref, *, repository, reference, max_groups=MAX_LAYER_GROUPS):
        """Publish one component per layer group, reusing any already published.

        Returns None when the image cannot be split (its Docker layers do not
        match the registry manifest); the caller then publishes the whole image.
        """
        if not isinstance(self.image_store, DockerOverlay2RootfsStore):
            raise ValueError("environment publication requires the immutable OCI build adapter")
        self.work_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        image_id = None
        try:
            # The lease pins the image, so Docker cannot remove the diff
            # directories while they are read.
            with self.image_store.operation_lease(image_ref) as source:
                image_id = source.image_id
                leased_id, diff_ids, directories = self.image_store.layer_diffs(image_ref)
                if leased_id != image_id:
                    raise ValueError("image changed while its layers were read")
                sizes = [layer.size for layer in self.registry.client.manifest_layers(repository, reference).layers]
                if len(sizes) != len(diff_ids):
                    _LOG.warning("registry manifest of %s lists %d layers, Docker %d; not splitting",
                                 image_ref, len(sizes), len(diff_ids))
                    return None
                layers = [(diff_id, directory, size) for diff_id, directory, size
                          in zip(diff_ids, directories, sizes) if diff_id != EMPTY_LAYER_DIFF_ID]
                if not layers:
                    return None
                layer_format = self.layer_format()
                components, reused = [], 0
                planned, _ = self._reusable_layer_groups(
                    [(diff_id, size) for diff_id, _, size in layers], layer_format, max_groups=max_groups)
                for _tag, _group, _parent, start, end in planned:
                    digest, hit = self._publish_layer_group(
                        [item[1] for item in layers[start:end]], [item[0] for item in layers[start:end]],
                        lower_dirs=[item[1] for item in layers[:start]],
                        parent=layer_chain_id(item[0] for item in layers[:start]), layer_format=layer_format)
                    components.append(digest)
                    reused += hit
                _LOG.info("published %s as %d layer components (%d reused)", image_ref, len(components), reused)
                return {"image_id": image_id, "image_config": source.image_config, "components": components,
                        "diff_ids": diff_ids, "reused": reused}
        finally:
            if image_id is not None:
                self._collect_image(image_id)

    @contextmanager
    def _group_lock(self, tag):
        # flock coordinates both threads and processes sharing this builder's
        # work root. Keep lock files: unlinking can split waiters across inodes.
        locks = self.work_root / "layer-locks"
        locks.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(locks / tag, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            with _phase("layer_lock_wait"):
                if remaining_build_execution_seconds() is None:
                    fcntl.flock(fd, fcntl.LOCK_EX)
                else:
                    while True:
                        remaining_build_execution_seconds()
                        try:
                            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            break
                        except BlockingIOError:
                            time.sleep(remaining_build_execution_seconds(0.05))
            yield
        finally:
            os.close(fd)

    def _publish_layer_group(self, directories, diff_ids, *, lower_dirs, parent, layer_format,
                             consume_private_diffs=False):
        tag = LAYER_TAG_PREFIX + layer_group_key(layer_format, parent, diff_ids)
        with self._group_lock(tag):
            return self._publish_claimed_layer_group(directories, diff_ids, lower_dirs=lower_dirs,
                parent=parent, layer_format=layer_format, consume_private_diffs=consume_private_diffs)

    def _publish_claimed_layer_group(self, directories, diff_ids, *, lower_dirs, parent, layer_format,
                                     consume_private_diffs=False):
        """Publish with the caller holding this exact group's filesystem claim."""
        tag = LAYER_TAG_PREFIX + layer_group_key(layer_format, parent, diff_ids)
        # Recheck after waiting: the preceding publisher may have filled it.
        with _phase("component_lookup"):
            existing = self._reuse_layer_component(tag, diff_ids, parent, layer_format)
        if existing is not None:
            _measure("groups_reused")
            return existing, True
        with TemporaryDirectory(dir=self.work_root) as temporary:
            root = Path(temporary)
            image = root / "component.erofs"
            if len(directories) == 1:
                view = directories[0]
                if view.is_symlink() or not view.is_dir():
                    raise ValueError("environment publication requires a real layer directory")
            else:
                view = root / "view"
                with _phase("squash"):
                    squash_layer_diffs(directories, view, lower_dirs=lower_dirs,
                                       consume_private_diffs=consume_private_diffs)
            with _phase("mkfs"):
                self._mkfs(image, view, exclude_runtime_mounts=True)
            with _phase("sign"):
                component = sign_layer_component(image, source_layers=diff_ids, parent=parent,
                                                 layer_format=layer_format, signing_key=self.signing_key)
            with _phase("publish_component"):
                digest = self.registry.publish(image, component, tag=tag)
            _measure("groups_built")
            _measure("erofs_bytes_built", component.image_size)
            return digest, False

    def _reuse_image_layers(self, repository, reference, *, max_groups):
        """Use signed components before Docker pulls or extracts any image data.

        Unsupported manifest layouts take the existing Docker path. Config
        bytes must match their OCI digest before they can bind a new signed root.
        """
        client = self.registry.client
        document, _ = client.manifest_document(repository, reference)
        descriptor = document.get("config", {})
        layers = document.get("layers")
        if (document.get("schemaVersion") != 2
                or document.get("mediaType") not in {OCI_IMAGE, "application/vnd.docker.distribution.manifest.v2+json"}
                or not isinstance(descriptor, dict)
                or type(descriptor.get("size")) is not int
                or not 0 < descriptor["size"] <= MAX_INDEX_BYTES
                or not isinstance(layers, list)):
            return None
        image_id = require_digest(descriptor.get("digest"))
        raw = client.blob_bytes(repository, image_id, max_bytes=descriptor["size"])
        if len(raw) != descriptor["size"] or content_digest(raw) != image_id:
            raise ValueError("source OCI config content identity mismatch")
        config = json.loads(raw)
        if not isinstance(config, dict):
            raise ValueError("invalid source OCI config")
        rootfs = config.get("rootfs", {})
        diff_ids = rootfs.get("diff_ids") if isinstance(rootfs, dict) else None
        if (not isinstance(diff_ids, list) or rootfs.get("type") != "layers"
                or len(diff_ids) != len(layers) or not diff_ids):
            return None
        for diff_id in diff_ids:
            require_digest(diff_id)
        if any(not isinstance(layer, dict) or type(layer.get("size")) is not int
               or layer["size"] < 0 for layer in layers):
            return None
        source = [(diff_id, layer["size"], layer) for diff_id, layer in zip(diff_ids, layers)
                  if diff_id != EMPTY_LAYER_DIFF_ID]
        if not source:
            return None
        image_config = DockerImageConfig.from_inspection(config.get("config"))
        try:
            layer_format = self.layer_format()
        except (OSError, subprocess.SubprocessError):
            # Older mkfs versions can still publish a whole-image component
            # through the existing fallback even if version discovery fails.
            return None
        groups, components = self._reusable_layer_groups(source, layer_format, max_groups=max_groups)
        _measure("preflight_misses", sum(component is None for component in components))
        if any(component is None for component in components):
            return self._materialize_registry_groups(repository, source, groups, components,
                image_id, image_config, diff_ids, layer_format)
        # Refresh only complete hits, retaining the existing blob-presence and
        # retention-grace check before committing the referencing root.
        for index, (tag, group, parent, _start, _end) in enumerate(groups):
            refreshed = self._reuse_layer_component(tag, group, parent, layer_format)
            if refreshed is None:
                _measure("preflight_misses")
                return None
            components[index] = refreshed
        _measure("groups_reused", len(components))
        _measure("docker_pull_skipped")
        return {"image_id": image_id, "image_config": image_config,
                "components": components, "diff_ids": diff_ids, "reused": len(components)}

    def _reusable_layer_groups(self, source, layer_format, *, max_groups):
        """Keep a cached trailing base group separate from a new small delta.

        Greedy byte grouping alone combines an unfinished (<64 MiB) base group
        with every new task layer. Probe a bounded number of shorter prefixes
        on misses, accepting only the existing signed, chain-bound components.
        Optional probes have a shared one-second budget and never increase the
        composition's existing device/group limit. Publication refreshes hits.
        """
        pending = list(plan_layer_groups([item[1] for item in source], max_groups=max_groups))
        groups, components = [], []
        probes = 0
        deadline = time.monotonic() + 1.0
        while pending:
            start, end = pending.pop(0)
            group = [item[0] for item in source[start:end]]
            parent = layer_chain_id(item[0] for item in source[:start])
            tag = LAYER_TAG_PREFIX + layer_group_key(layer_format, parent, group)
            component = self._reuse_layer_component(tag, group, parent, layer_format, refresh=False)
            if component is None and len(groups) + len(pending) + 1 < max_groups:
                for split in range(end - 1, start, -1):
                    remaining = deadline - time.monotonic()
                    if probes >= 16 or remaining <= 0:
                        break
                    prefix = [item[0] for item in source[start:split]]
                    prefix_tag = LAYER_TAG_PREFIX + layer_group_key(layer_format, parent, prefix)
                    probes += 1
                    _measure("prefix_component_probes")
                    try:
                        with build_execution_deadline(remaining):
                            hit = self._reuse_layer_component(prefix_tag, prefix, parent, layer_format, refresh=False)
                    except (ImageBuildTimeoutError, OSError, RegistryRequestError):
                        # A speculative miss must not turn a build into a failure.
                        # The authoritative publication path keeps normal errors.
                        deadline = 0
                        break
                    if hit is not None:
                        pending.insert(0, (split, end))
                        tag, group, component, end = prefix_tag, prefix, hit, split
                        _measure("prefix_components_reused")
                        break
            groups.append((tag, group, parent, start, end))
            components.append(component)
        return groups, components

    def _materialize_registry_groups(self, repository, source, groups, components,
                                     image_id, image_config, diff_ids, layer_format):
        """Claim shared misses before downloading or extracting their OCI bytes.

        Claim tags in one order across images, and retain only still-missing
        claims. Docker publication takes one claim at a time; this selective
        path holds its bounded set until all extraction has been validated and
        publication completes. The claimed publisher must not lock them again.
        """
        if not any(components):
            return None
        missing = [index for index, component in enumerate(components) if component is None]
        with ExitStack() as claims:
            for index in sorted(missing, key=lambda index: groups[index][0]):
                tag, group, parent, _start, _end = groups[index]
                with ExitStack() as claim:
                    claim.enter_context(self._group_lock(tag))
                    with _phase("component_lookup"):
                        component = self._reuse_layer_component(tag, group, parent, layer_format)
                    if component is not None:
                        components[index] = component
                    else:
                        claims.enter_context(claim.pop_all())
            if all(component is not None for component in components):
                # Refresh preflight hits too: waiting may outlast their GC grace.
                for index, (tag, group, parent, _start, _end) in enumerate(groups):
                    refreshed = self._reuse_layer_component(tag, group, parent, layer_format)
                    if refreshed is None:
                        return None
                    components[index] = refreshed
                _measure("groups_reused", len(components))
                _measure("docker_pull_skipped")
                return {"image_id": image_id, "image_config": image_config, "components": components,
                        "diff_ids": diff_ids, "reused": len(components)}
            return self._materialize_claimed_registry_groups(repository, source, groups, components,
                image_id, image_config, diff_ids, layer_format)

    def _materialize_claimed_registry_groups(self, repository, source, groups, components,
                                             image_id, image_config, diff_ids, layer_format):
        """Fetch only small missing groups when no lower filesystem is needed.

        Cold images and unsupported diffs use the existing Docker path. The
        direct extractor authenticates both compressed and uncompressed bytes,
        refuses lower-dependent semantics, and completes before any component
        is signed. Cached groups are never downloaded or unpacked here.
        """
        from .oci_layer_materialize import UnsupportedLayer, materialize_layers

        if not any(components):
            return None
        missing = [index for index, component in enumerate(components) if component is None]
        selected = [item for index in missing for item in source[groups[index][3]:groups[index][4]]]
        self.work_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        with TemporaryDirectory(dir=self.work_root) as temporary:
            prepared = None
            try:
                if self.preparation_subprocess:
                    from .environment_prepare import prepare_in_subprocess
                    result = prepare_in_subprocess(self.registry.client, repository,
                        [item[2] for item in selected], [item[0] for item in selected],
                        [groups[index][4] - groups[index][3] for index in missing], Path(temporary),
                        timeout_seconds=remaining_build_execution_seconds(600))
                    for name, value in result.metrics.items():
                        _measure(name, value)
                    if result.fallback:
                        raise UnsupportedLayer("isolated selective extraction requires Docker",
                                               reason=result.fallback_reason or "unsupported")
                    prepared = dict(zip(missing, result.views))
                else:
                    transfer_metrics = {}
                    try:
                        with _phase("selective_materialization"):
                            directories = materialize_layers(self.registry.client, repository,
                                [item[2] for item in selected], [item[0] for item in selected],
                                Path(temporary), metrics=transfer_metrics)
                    finally:
                        for name, value in transfer_metrics.items():
                            _measure(name, value)
            except (UnsupportedLayer, OSError, EOFError, tarfile.TarError) as exc:
                _measure("selective_fallbacks")
                reason = (exc.reason if isinstance(exc, UnsupportedLayer) else
                          "io" if isinstance(exc, OSError) else "archive")
                _measure("selective_fallback_" + reason)
                _LOG.info("selective layer materialization requires Docker: %s", reason)
                return None
            offset, reused = 0, 0
            for index, (tag, group, parent, start, end) in enumerate(groups):
                if components[index] is not None:
                    refreshed = self._reuse_layer_component(tag, group, parent, layer_format)
                    if refreshed is None:
                        # Its blob was swept while the missing diff was read.
                        return None
                    components[index] = refreshed
                    _measure("groups_reused")
                    reused += 1
                    continue
                count = end - start
                views = [prepared[index]] if prepared is not None else directories[offset:offset + count]
                component, hit = self._publish_claimed_layer_group(views, group,
                    lower_dirs=(), parent=parent, layer_format=layer_format, consume_private_diffs=True)
                components[index] = component
                reused += hit
                offset += count
            _measure("selective_materializations")
            _measure("oci_layers_materialized", len(selected))
            _measure("oci_download_bytes", sum(item[1] for item in selected))
            _measure("docker_pull_skipped")
            return {"image_id": image_id, "image_config": image_config, "components": components,
                    "diff_ids": diff_ids, "reused": reused}

    def _reuse_layer_component(self, tag, diff_ids, parent, layer_format, *, refresh=True):
        """The published component for this group, or None to build it.

        The tag is only an index: a stale, foreign or unloadable entry is
        rebuilt and overwritten. A hit re-puts the tag, restarting its
        retention grace period until the new root references the component.
        """
        client, repository = self.registry.client, self.registry.repository
        try:
            document, _headers = client.manifest_document(repository, tag)
            payload = canonical_bytes(document)
            digest = content_digest(payload)
            component = self.registry.load_document(digest, document)
        except (RegistryRequestError, ValueError) as exc:
            if isinstance(exc, RegistryRequestError) and exc.status_code not in {400, 404}:
                raise
            return None
        if (not isinstance(component, LayerEnvironmentComponent) or component.source_layers != tuple(diff_ids)
                or component.parent != parent or component.format != layer_format):
            _LOG.warning("environment tag %s names another component; rebuilding it", tag)
            return None
        if not refresh:
            return digest
        try:
            # The registry refuses this when a blob is gone (swept after the
            # manifest was deleted); the group is then built again.
            client.put_manifest(repository, tag, payload, media_type=OCI_IMAGE)
        except RegistryRequestError as exc:
            if exc.status_code not in {400, 404}:
                raise
            return None
        return digest

    def publish_image(self, image_ref, *, allowlist, toolkits=()):
        with publication_metrics() as metrics:
            try:
                with _phase("total"):
                    return self._publish_image(image_ref, allowlist=allowlist, toolkits=toolkits)
            finally:
                if self.release_published_tag:
                    # The registry owns the durable artifact. Docker was only
                    # a temporary extraction cache; rootfs collection releases
                    # its pin but does not remove the pulled publication tag.
                    # Remove just this tag, without force: other tags, reader
                    # pins and containers continue to protect shared layers.
                    try:
                        with without_build_execution_deadline(), build_execution_deadline(30):
                            self.image_store._checked(self.image_store.docker_binary,
                                "image", "rm", image_ref, timeout=30)
                    except (DirectWardenError, OSError, subprocess.SubprocessError, ImageBuildTimeoutError):
                        _LOG.warning("temporary publication tag cleanup deferred: %s", image_ref)
                _LOG.info("environment publication metrics %s", json.dumps(metrics, sort_keys=True))

    def _publish_image(self, image_ref, *, allowlist, toolkits=()):
        """Publish fresh build output and attach it to the existing image tag."""
        import uuid
        from .environment_artifact import attach_environment_to_image, publish_environment
        from .environment_manifest import EnvironmentManifest
        from .managed_registry import registry_repository_tag_from_image_ref
        coordinates = registry_repository_tag_from_image_ref(image_ref)
        if coordinates is None or "@" in image_ref:
            raise ValueError("environment publication requires an owned image tag")
        repository, tag = coordinates
        toolkits = tuple(toolkits)
        allowlist = tuple(allowlist)
        max_groups = min(MAX_LAYER_GROUPS, 33 - len(toolkits))
        layered = None
        if _whole_image(allowlist):
            with _phase("preflight"):
                layered = self._reuse_image_layers(repository, tag, max_groups=max_groups)
        if layered is None:
            with _phase("docker_pull"):
                self.image_store._checked(self.image_store.docker_binary, "pull", image_ref, timeout=600)
        if layered is None and _whole_image(allowlist):
            try:
                # The manifest holds the base and at most 32 more components.
                layered = self.build_layers(image_ref, repository=repository, reference=tag,
                                            max_groups=max_groups)
            except RegistryRequestError:
                raise
            except (ValueError, DirectWardenError, subprocess.SubprocessError, OSError) as exc:
                _LOG.warning("per-layer environment publication of %s failed; publishing the whole image: %s",
                             image_ref, exc)
        if layered is not None:
            result, diff_ids = layered, layered["diff_ids"]
            manifest = EnvironmentManifest(layered["components"][0],
                                           toolkits=(*layered["components"][1:], *toolkits))
        else:
            result = self.build(image_ref, allowlist=allowlist, tag="environment-component-" + uuid.uuid4().hex)
            diff_ids, manifest = None, EnvironmentManifest(result["component_digest"], toolkits=toolkits)
        config = result["image_config"]
        environment_digest = publish_environment(self.registry,
            source_image=result["image_id"], environment=manifest,
            image_config={"Entrypoint": list(config.entrypoint), "Cmd": list(config.command), "Env": list(config.env),
                          "WorkingDir": config.working_dir, "User": config.user},
            signing_key=self.signing_key, tag="environment-root-" + uuid.uuid4().hex, source_diff_ids=diff_ids)
        return attach_environment_to_image(self.registry, image_repository=repository, image_reference=tag,
                                           environment_digest=environment_digest)


def main(argv=None):
    """Publish one canary through the same builder implementation as image builds."""
    import argparse
    from .environment_config import add_environment_registry_args
    parser = argparse.ArgumentParser(description="Publish a fresh allowlisted immutable image artifact")
    parser.add_argument("--image-ref", required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--docker-binary", default="docker")
    parser.add_argument("--environment-signing-key", type=Path, required=True)
    parser.add_argument("--environment-allow-path", action="append", default=[])
    add_environment_registry_args(parser)
    args = parser.parse_args(argv)
    return publish_from_args(args)


def publish_from_args(args):
    import json
    from types import SimpleNamespace
    from .environment_artifact import load_image_environment
    from .environment_config import environment_publisher_from_args, environment_registry_from_args
    from .managed_registry import registry_repository_tag_from_image_ref
    args.image_file = args.state_root / "images.sqlite"
    publisher = environment_publisher_from_args(args)
    digest = publisher(SimpleNamespace(tag=args.image_ref))
    repository, _ = registry_repository_tag_from_image_ref(args.image_ref)
    root, environment = load_image_environment(environment_registry_from_args(args), repository, digest)
    print(json.dumps({"image_ref": args.image_ref + "@" + digest, "manifest_digest": digest,
                      "environment_root": root, "components": environment.components}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
