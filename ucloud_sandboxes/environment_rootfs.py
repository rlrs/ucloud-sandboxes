"""Immutable artifact adapter for the existing OverlayRootfsManager.

OCI image refs remain the input. Signed metadata selects immutable components;
only their demanded blocks are fetched by the independent artifact-I/O backend.
The disk-backed writable overlay and sandbox lifecycle have one implementation.
"""
from contextlib import ExitStack, contextmanager
from collections import OrderedDict
from concurrent.futures import Future
import fcntl
import json
import os
from pathlib import Path
import shutil
from urllib.parse import urlparse
from threading import Lock

from .environment_artifact import (ImmutableEnvironment, canonical_bytes,
    environment_root_digest, load_image_environment, require_digest)
from .environment_backend import mount_has_dependents
from .environment_manifest import HOST_EROFS_ABI
from .image_rootfs import (
    DockerImageConfig, MaterializedRootfs, SubprocessCommandRunner,
    _atomic_write, _mount_present, _require_private_directory,
)
from .managed_registry import manifest_digest_from_image_ref, registry_host_from_image_ref, registry_repository_tag_from_image_ref


class EnvironmentRootfsStore:
    backend_abi = HOST_EROFS_ABI

    def __init__(self, root, registry, backend, *, runner=None, referenced=None):
        self.root, self.registry, self.backend = Path(root), registry, backend
        if not self.root.is_absolute():
            raise ValueError("environment image store must be absolute")
        self.images, self.locks = self.root / "images", self.root / "locks"
        for path in (self.root, self.images, self.locks):
            path.mkdir(parents=True, mode=0o700, exist_ok=True)
            _require_private_directory(path)
        self.runner = runner or SubprocessCommandRunner()
        # Removing an image view must fence its OverlayFS users. Sibling binds
        # may remain: only the backend disconnects backing, and it separately
        # checks every filesystem peer before releasing the block device.
        self._referenced = referenced or (lambda path: mount_has_dependents(path, include_bind_mounts=False))
        self._metrics_guard = Lock()
        self._active_leases = self._waiting_leases = 0
        self._resolutions = OrderedDict()
        self._resolving = {}

    @contextmanager
    def _lease(self, image_id, exclusive=False):
        require_digest(image_id)
        descriptor = os.open(self.locks / (image_id[7:] + ".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        acquired = False
        with self._metrics_guard:
            self._waiting_leases += 1
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            with self._metrics_guard:
                self._waiting_leases -= 1
                self._active_leases += 1
            acquired = True
            yield
        finally:
            with self._metrics_guard:
                if acquired:
                    self._active_leases -= 1
                else:
                    self._waiting_leases -= 1
            os.close(descriptor)

    def _mounted(self, root):
        return _mount_present(root, self.runner, "mountpoint")

    def _load(self, image_id):
        receipt = json.loads((self.images / image_id[7:] / "environment.json").read_bytes())
        if not isinstance(receipt, dict) or set(receipt) != {"root", "source", "environment"}:
            raise ValueError("invalid immutable image receipt")
        environment = ImmutableEnvironment.from_dict(receipt["environment"]).authenticate(self.registry.trusted_keys)
        if environment_root_digest(environment) != receipt["root"]:
            raise ValueError("environment receipt root identity changed")
        if "sha256:" + environment.environment.sha256 != image_id:
            raise ValueError("environment image identity changed")
        return environment, receipt

    def _mount(self, image_id, environment):
        # Hold every component against GC until OverlayFS takes kernel refs.
        with ExitStack() as leases:
            for digest in sorted(set(environment.components)):
                leases.enter_context(self._lease(digest))
            return self._mount_components(image_id, environment)

    def _mount_components(self, image_id, environment):
        target = self.images / image_id[7:]
        rootfs = target / "rootfs"
        rootfs.mkdir(mode=0o700, exist_ok=True)
        if self._mounted(rootfs):
            # Backend ensure is also a liveness/fencing check after frontend
            # restart. Never silently trust retained mounts whose I/O died.
            for component in environment.components:
                self.backend.ensure(component)
            return rootfs
        lowers = [self.backend.ensure(digest) for digest in environment.components]
        if any(any(character in str(path) for character in (":", ",", "\n")) for path in lowers):
            raise ValueError("invalid component mount path")
        if len(lowers) == 1:
            # Linux requires two lowers for an OverlayFS without an upper.
            # One immutable component already has the required filesystem view;
            # binding its read-only EROFS superblock preserves that protection.
            command = ("mount", "--bind", str(lowers[0]), str(rootfs))
        else:
            command = ("mount", "-t", "overlay", "overlay", "-o",
                "ro,lowerdir=" + ":".join(str(path) for path in reversed(lowers)), str(rootfs))
        result = self.runner.run(command, timeout=60)
        if result.returncode:
            raise RuntimeError(f"immutable environment mount failed: {result.stderr or result.stdout}")
        return rootfs

    def _resolved(self, image_ref):
        coordinates = registry_repository_tag_from_image_ref(image_ref)
        expected_host = urlparse(self.registry.client.base_url).netloc
        if coordinates is None or registry_host_from_image_ref(image_ref) != expected_host:
            raise ValueError("immutable environment requires the configured managed image registry")
        repository, tag = coordinates
        pinned = manifest_digest_from_image_ref(image_ref)
        reference = pinned or tag
        key = (repository, pinned)
        pending = None
        if pinned:
            with self._metrics_guard:
                if key in self._resolutions:
                    self._resolutions.move_to_end(key)
                    root, environment = self._resolutions[key]
                    return environment, {"root": root, "source": image_ref, "environment": environment.to_dict()}
                pending = self._resolving.get(key)
                owns_resolution = pending is None
                if owns_resolution:
                    pending = Future()
                    self._resolving[key] = pending
            if not owns_resolution:
                root, environment = pending.result()
                return environment, {"root": root, "source": image_ref, "environment": environment.to_dict()}
        try:
            root, environment = load_image_environment(self.registry, repository, reference)
            if pending is not None:
                with self._metrics_guard:
                    self._resolutions[key] = (root, environment)
                    self._resolutions.move_to_end(key)
                    while len(self._resolutions) > 128:
                        self._resolutions.popitem(last=False)
                pending.set_result((root, environment))
        except BaseException as exc:
            if pending is not None:
                pending.set_exception(exc)
            raise
        finally:
            if pending is not None:
                with self._metrics_guard:
                    self._resolving.pop(key, None)
        return environment, {"root": root, "source": image_ref, "environment": environment.to_dict()}

    @contextmanager
    def operation_lease(self, image_ref):
        environment, receipt = self._resolved(image_ref)
        image_id = "sha256:" + environment.environment.sha256
        while True:
            with self._lease(image_id):
                target = self.images / image_id[7:]
                rootfs = target / "rootfs"
                if (target / "environment.json").exists() and self._mounted(rootfs):
                    existing, _ = self._load(image_id)
                    if existing.to_dict() != environment.to_dict():
                        raise ValueError("environment config changed for an existing composition")
                    self._mount(image_id, environment)
                    yield MaterializedRootfs(image_ref, image_id,
                        environment.environment.rootfs_fingerprint(HOST_EROFS_ABI), rootfs,
                        DockerImageConfig.from_inspection(environment.image_config), environment.environment, HOST_EROFS_ABI)
                    return
            with self._lease(image_id, exclusive=True):
                target.mkdir(mode=0o700, exist_ok=True)
                _atomic_write(target / "environment.json", canonical_bytes(receipt))
                self._mount(image_id, environment)

    @contextmanager
    def mounted_rootfs_lease(self, image_id, *, rootfs_identity_sha256):
        while True:
            with self._lease(image_id):
                environment, receipt = self._load(image_id)
                if environment.environment.rootfs_fingerprint(HOST_EROFS_ABI) != rootfs_identity_sha256:
                    raise ValueError("environment resume fingerprint changed")
                rootfs = self.images / image_id[7:] / "rootfs"
                if self._mounted(rootfs):
                    self._mount(image_id, environment)
                    yield rootfs
                    return
            # Recovery never re-resolves a mutable OCI tag: the receipt binds
            # the exact signed root selected before initial durable admission.
            with self._lease(image_id, exclusive=True):
                environment, _ = self._load(image_id)
                self._mount(image_id, environment)

    def warm(self, image_ref):
        with self.operation_lease(image_ref):
            pass

    def collect_image(self, image_id, *, is_referenced):
        with self._lease(image_id, exclusive=True):
            if is_referenced(image_id):
                return False
            target = self.images / image_id[7:]
            if not target.exists():
                return False
            environment, _ = self._load(image_id)
            rootfs = target / "rootfs"
            if self._mounted(rootfs):
                if self._referenced(rootfs):
                    return False
                result = self.runner.run(("umount", str(rootfs)), timeout=60)
                if result.returncode:
                    return False
            shutil.rmtree(target)
            for digest in environment.components:
                with self._lease(digest, exclusive=True):
                    self.backend.drop(digest)  # Other composed lowers return EBUSY.
            return True

    def reconcile_images(self, image_ids, *, is_referenced):
        roots = frozenset(require_digest(image_id) for image_id in image_ids)
        if any(not (self.images / image_id[7:]).is_dir() for image_id in roots):
            raise ValueError("direct registry references a missing environment receipt")
        collected = retained = 0
        def keep(candidate):
            return candidate in roots or is_referenced(candidate)
        for target in tuple(self.images.iterdir()):
            image_id = require_digest("sha256:" + target.name)
            if self.collect_image(image_id, is_referenced=keep):
                collected += 1
                continue
            with self._lease(image_id, exclusive=True):
                if target.exists():
                    environment, _ = self._load(image_id)
                    # Retained image roots must have a live I/O owner before a
                    # restarted node agent can advertise healthy admission.
                    self._mount(image_id, environment)
                    retained += 1
        return {"collected": collected, "retained": retained}

    def operation_snapshot(self):
        with self._metrics_guard:
            return {"active_operations": self._active_leases, "waiting_operations": self._waiting_leases}


class EnvironmentImageRuntime:
    """Existing image API input adapter; pulls metadata/demanded bytes, not OCI layers."""
    dry_run = False
    materializes_rootfs = True
    pull_phase = "environment_resolve"

    def __init__(self, image_store):
        self.image_store = image_store

    def pull(self, image):
        from .sandbox import CommandResult
        self.image_store.warm(image)
        return CommandResult(argv=("immutable-environment", "resolve", image), exit_code=0)

    def build(self, *args, **kwargs):
        raise ValueError("worker image builds are disabled; use the trusted builder")

    def push(self, *args, **kwargs):
        raise ValueError("immutable environment workers do not publish images")
