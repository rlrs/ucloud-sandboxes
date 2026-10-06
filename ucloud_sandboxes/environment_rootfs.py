"""Immutable artifact adapter for the existing OverlayRootfsManager.

OCI image refs remain the input. Signed metadata selects immutable components;
only their demanded blocks are fetched by the independent artifact-I/O backend.
The disk-backed writable overlay and sandbox lifecycle have one implementation.
"""
from contextlib import ExitStack, contextmanager
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
import fcntl
import json
import logging
import os
from pathlib import Path
import shutil
import time
from urllib.parse import urlparse
from threading import Lock

from .environment_artifact import (ImmutableEnvironment, canonical_bytes,
    environment_root_digest, load_dispatched_environment, load_image_environment, require_digest)
from .direct_registry import DirectRegistryCapacityUnavailable
from .environment_backend import NO_BLOCK_DEVICE, block_device_count, mount_has_dependents
from .environment_config import DEFAULT_DEVICE_BUDGET_PERCENT
from .environment_manifest import HOST_EROFS_ABI
from .image_rootfs import (
    DockerImageConfig, MaterializedRootfs, SubprocessCommandRunner,
    _atomic_write, _mount_present, _require_private_directory,
)
from .managed_registry import manifest_digest_from_image_ref, registry_host_from_image_ref, registry_repository_tag_from_image_ref
from .models import environment_io_metrics

_LOG = logging.getLogger(__name__)
# A composition used this recently may back a create whose registration does
# not exist yet: noded leases what the agent materialized only after the
# materialize response, and a gateway pull warms an image before its create.
IDLE_GRACE_SECONDS = 60.0


class EnvironmentDeviceCapacityError(DirectRegistryCapacityUnavailable):
    """Every environment block device serves another component."""


class EnvironmentRootfsStore:
    """Compositions outlive their sandboxes: a deleted sandbox's image stays
    mounted for the next sandbox of that image (re-materializing costs a
    backend attach per component, an overlay mount and a receipt). The sweep
    (``collect_idle``) collects unreferenced compositions, least recently used
    first, only while the node is over budget: attached components above
    ``device_budget_percent`` of the block devices, or a device cache with no
    eviction of its own (nydusd) over its bytes."""
    backend_abi = HOST_EROFS_ABI
    retains_idle_images = True

    def __init__(self, root, registry, backend, *, runner=None, referenced=None, block_devices=None, rafs=False,
                 device_budget_percent=DEFAULT_DEVICE_BUDGET_PERCENT, clock=time.time):
        self.root, self.registry, self.backend = Path(root), registry, backend
        self.rafs = bool(rafs)  # The backend reads RAFS components (a chunk index and store node).
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
        # The backend serves one block device per distinct mounted component
        # from the whole nodewide NBD pool; count both for admission metrics.
        self._block_devices = block_device_count() if block_devices is None else int(block_devices)
        self._mounted_components = {}
        if type(device_budget_percent) is not int or not 1 <= device_budget_percent <= 100:
            raise ValueError("environment device budget must be a percentage from 1 to 100")
        self.device_budget = self._block_devices * device_budget_percent // 100
        self.clock = clock
        self._last_used = {}  # Image id -> wall-clock time of its last lease, create or delete.
        self._sweep_guard = Lock()
        # Registry ownership, bound by the provisioner: a sweep after device
        # exhaustion starts inside a mount, where no caller passes it.
        self.is_referenced = None

    @contextmanager
    def _lease(self, image_id, exclusive=False, blocking=True):
        """Yields whether the lock is held: only ``blocking=False`` yields False."""
        require_digest(image_id)
        descriptor = os.open(self.locks / (image_id[7:] + ".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        acquired = False
        with self._metrics_guard:
            self._waiting_leases += 1
        try:
            try:
                fcntl.flock(descriptor, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
                            | (0 if blocking else fcntl.LOCK_NB))
            except BlockingIOError:
                yield False
                return
            with self._metrics_guard:
                self._waiting_leases -= 1
                self._active_leases += 1
            acquired = True
            yield True
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
        try:
            return self._mount_once(image_id, environment)
        except EnvironmentDeviceCapacityError:
            # Idle compositions hold devices for their next sandbox: collect
            # the least recently used and try once more. The sweep skips this
            # image, whose lease the caller holds.
            if not self.collect_idle(free_devices=len(set(environment.components)), wait=True):
                raise
            _LOG.info("collected idle environment images after device exhaustion; retrying %s", image_id)
        return self._mount_once(image_id, environment)

    def _mount_once(self, image_id, environment):
        # Hold every component against GC until OverlayFS takes kernel refs.
        components = sorted(set(environment.components))
        try:
            with ExitStack() as leases:
                for digest in components:
                    leases.enter_context(self._lease(digest))
                return self._mount_components(image_id, environment)
        except EnvironmentDeviceCapacityError:
            # A many-layer image can exhaust the pool halfway through mounting.
            # Release unused exports before placement retries elsewhere, or the
            # failed admission itself keeps the node full. Upgrade to exclusive
            # leases only after releasing the shared ones; concurrent composers
            # finish first, and the backend retains their kernel dependencies.
            for digest in components:
                try:
                    with self._lease(digest, exclusive=True):
                        self.backend.drop(digest)
                except Exception:
                    _LOG.warning("could not release component after device exhaustion: %s", digest, exc_info=True)
            raise

    def _ensure(self, digest):
        try:
            return self.backend.ensure(digest)
        except RuntimeError as exc:
            # The backend RPC reports errors as text. Device exhaustion is a
            # node capacity limit: placement may try another worker.
            if NO_BLOCK_DEVICE in str(exc) and not isinstance(exc, EnvironmentDeviceCapacityError):
                raise EnvironmentDeviceCapacityError(str(exc)) from exc
            raise

    def _track(self, image_id, environment):
        with self._metrics_guard:
            self._mounted_components[image_id] = tuple(environment.components)

    def _mount_components(self, image_id, environment):
        target = self.images / image_id[7:]
        rootfs = target / "rootfs"
        rootfs.mkdir(mode=0o700, exist_ok=True)
        if self._mounted(rootfs):
            # Backend ensure is also a liveness/fencing check after frontend
            # restart. Never silently trust retained mounts whose I/O died.
            for component in environment.components:
                self._ensure(component)
            self._track(image_id, environment)
            return rootfs
        # The backend attaches distinct components concurrently (one
        # single flight each), so a per-layer image does not pay for its
        # layers one after another.
        if len(environment.components) == 1:
            lowers = [Path(self._ensure(environment.components[0]))]
        else:
            with ThreadPoolExecutor(min(8, len(environment.components)), thread_name_prefix="ensure") as pool:
                lowers = [Path(path) for path in pool.map(self._ensure, environment.components)]
        if any(any(character in str(path) for character in (":", ",", "\n")) for path in lowers):
            raise ValueError("invalid component mount path")
        if len(lowers) == 1:
            # Linux requires two lowers for an OverlayFS without an upper.
            # One immutable component already has the required filesystem view;
            # binding its read-only EROFS superblock preserves that protection.
            command = ("mount", "--bind", str(lowers[0]), str(rootfs))
        else:
            parent = lowers[0].parent
            if any(path.parent != parent for path in lowers) or not parent.is_absolute():
                raise ValueError("environment components must share one mount directory")
            # A per-layer image stacks up to 33 components. Relative names
            # from the shared components directory keep the option far below
            # the one-page mount(2) limit; the forced classic mount(2) avoids
            # fsconfig's 256-byte option values. mount_has_dependents resolves
            # these names against the same directory.
            command = ("env", "-C", str(parent), "LIBMOUNT_FORCE_MOUNT2=always",
                "mount", "-t", "overlay", "overlay", "-o",
                "ro,lowerdir=" + ":".join(path.name for path in reversed(lowers)), str(rootfs))
        result = self.runner.run(command, timeout=60)
        if result.returncode:
            raise RuntimeError(f"immutable environment mount failed: {result.stderr or result.stdout}")
        self._track(image_id, environment)
        return rootfs

    def _resolved(self, image_ref, environment_root=None):
        coordinates = registry_repository_tag_from_image_ref(image_ref)
        expected_host = urlparse(self.registry.client.base_url).netloc
        if coordinates is None or registry_host_from_image_ref(image_ref) != expected_host:
            raise ValueError("immutable environment requires the configured managed image registry")
        repository, tag = coordinates
        pinned = manifest_digest_from_image_ref(image_ref)
        reference = pinned or tag
        key = (repository, pinned, environment_root)
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
            root, environment = (
                load_dispatched_environment(self.registry, repository, reference, environment_root)
                if environment_root else load_image_environment(self.registry, repository, reference))
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
    def operation_lease(self, image_ref, environment_root=None):
        environment, receipt = self._resolved(image_ref, environment_root)
        with self._leased(image_ref, environment, receipt) as rootfs:
            yield rootfs

    def materialize_resolution(self, image_ref, environment_root=None):
        """Mount the image and return the resolution the node daemon leases it by."""
        environment, receipt = self._resolved(image_ref, environment_root)
        with self._leased(image_ref, environment, receipt):
            return receipt

    @contextmanager
    def _leased(self, image_ref, environment, receipt):
        image_id = "sha256:" + environment.environment.sha256
        while True:
            with self._lease(image_id):
                target = self.images / image_id[7:]
                rootfs = target / "rootfs"
                if (target / "environment.json").exists() and self._mounted(rootfs):
                    existing, _ = self._load(image_id)
                    # A composition is a filesystem; config-only siblings share it.
                    if existing.environment != environment.environment:
                        raise ValueError("environment components changed for an existing composition")
                    self._mount(image_id, environment)
                    self.note_used(image_id)
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
                    self.note_used(image_id)
                    yield rootfs
                    return
            # Recovery never re-resolves a mutable OCI tag: the receipt binds
            # the exact signed root selected before initial durable admission.
            with self._lease(image_id, exclusive=True):
                environment, _ = self._load(image_id)
                self._mount(image_id, environment)

    def warm(self, image_ref, environment_root=None):
        with self.operation_lease(image_ref, environment_root):
            pass

    def collect_image(self, image_id, *, is_referenced):
        with self._lease(image_id, exclusive=True):
            if is_referenced(image_id):
                return False
            return self._collect_locked(image_id)

    def _collect_locked(self, image_id):
        """Unmount and drop one unreferenced composition; its exclusive lease is held."""
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
        with self._metrics_guard:
            self._mounted_components.pop(image_id, None)
            self._last_used.pop(image_id, None)
        for digest in environment.components:
            with self._lease(digest, exclusive=True):
                self.backend.drop(digest)  # Other composed lowers return EBUSY.
        return True

    def note_used(self, image_id):
        """An image was just leased, or a sandbox of it created or deleted: the sweep's LRU order."""
        with self._metrics_guard:
            self._last_used[image_id] = self.clock()

    def _pressure(self):
        """(attached components, device cache over its bytes) from the backend;
        this process's mounted view when the backend cannot say (an older one)."""
        try:
            raw = self.backend.pressure()
            active, over_cache = raw["active_components"], raw["over_cache_budget"]
            if type(active) is int and active >= 0 and type(over_cache) is bool:
                return active, over_cache
        except (AttributeError, KeyError, TypeError, OSError, RuntimeError, ValueError):
            pass
        return self.operation_snapshot()["environment_devices_in_use"], False

    def collect_idle(self, *, free_devices=0, wait=False):
        """Collect unreferenced compositions, least recently used first, while
        the node is over budget; return their image ids.

        ``free_devices`` instead frees just that many block devices (an
        attach found the pool exhausted; the reconciler's next sweep restores
        the budget off the create path). An image a create, materialization
        or noded lease holds is skipped, never waited for; one used in the
        last ``IDLE_GRACE_SECONDS`` is kept.
        """
        is_referenced = self.is_referenced
        if is_referenced is None or not self._sweep_guard.acquire(blocking=wait):
            return ()
        try:
            return self._collect_idle(is_referenced, free_devices)
        finally:
            self._sweep_guard.release()

    def _collect_idle(self, is_referenced, free_devices):
        limit = self._block_devices - free_devices if free_devices else self.device_budget

        def over_budget():
            active, over_cache = self._pressure()
            return over_cache or (self._block_devices > 0 and active > limit)

        if not over_budget():
            return ()
        with self._metrics_guard:
            seen = dict(self._last_used)

        def last_used(image_id):
            # After a restart, the receipt's write time: its last materialization.
            if image_id in seen:
                return seen[image_id]
            try:
                return (self.images / image_id[7:] / "environment.json").stat().st_mtime
            except OSError:
                return 0.0

        found = {"sha256:" + path.name: None for path in self.images.iterdir() if len(path.name) == 64}
        order = sorted(((last_used(image_id), image_id) for image_id in found))
        now, collected = self.clock(), []
        for used, image_id in order:
            if now - used < IDLE_GRACE_SECONDS:
                break  # LRU order: every later image is as recent.
            try:
                with self._lease(image_id, exclusive=True, blocking=False) as owned:
                    if not owned:
                        continue  # Leased: being created from or materialized.
                    if is_referenced(image_id):
                        # A live sandbox (noded's creates included) uses it now.
                        self.note_used(image_id)
                        continue
                    if not self._collect_locked(image_id):
                        continue
            except Exception as exc:  # One bad image must not stop the sweep.
                _LOG.warning("could not collect idle environment image %s: %s", image_id, exc)
                continue
            collected.append(image_id)
            if not over_budget():
                break
        if collected:
            _LOG.info("collected %d idle environment image(s) over the node budget", len(collected))
        return tuple(collected)

    def _retain_idle(self, image_id):
        """Keep a mounted, unregistered composition whose I/O is live as an
        idle cache entry; its exclusive lease is held."""
        target = self.images / image_id[7:]
        if not (target / "environment.json").exists() or not self._mounted(target / "rootfs"):
            return False
        environment, _ = self._load(image_id)
        try:
            for component in environment.components:
                self._ensure(component)
        except (OSError, RuntimeError, ValueError):
            return False  # Dead or fenced I/O: collect it.
        self._track(image_id, environment)
        return True

    def reconcile_images(self, image_ids, *, is_referenced):
        """Restart and periodic reconciliation. A registered image is mounted
        and its I/O checked. An unregistered composition that is still mounted
        with live I/O stays as an idle cache entry, for the budget sweep to
        collect; anything else (unmounted, half-materialized, dead I/O) goes."""
        roots = frozenset(require_digest(image_id) for image_id in image_ids)
        if any(not (self.images / image_id[7:]).is_dir() for image_id in roots):
            raise ValueError("direct registry references a missing environment receipt")
        collected = retained = idle = 0
        for target in tuple(self.images.iterdir()):
            image_id = require_digest("sha256:" + target.name)
            with self._lease(image_id, exclusive=True):
                if not target.exists():
                    continue
                if image_id in roots or is_referenced(image_id):
                    environment, _ = self._load(image_id)
                    # Retained image roots must have a live I/O owner before a
                    # restarted node agent can advertise healthy admission.
                    self._mount(image_id, environment)
                    retained += 1
                elif self._retain_idle(image_id):
                    idle += 1
                elif self._collect_locked(image_id):
                    collected += 1
        return {"collected": collected, "retained": retained, "idle": idle}

    def operation_snapshot(self):
        with self._metrics_guard:
            # Images sharing a base share its components and their devices.
            in_use = len({digest for components in self._mounted_components.values() for digest in components})
            return {"active_operations": self._active_leases, "waiting_operations": self._waiting_leases,
                    "environment_devices_total": self._block_devices,
                    "environment_devices_in_use": in_use,
                    "environment_devices_free": max(0, self._block_devices - in_use)}

    def io_metrics(self):
        """Nodewide backend counters for heartbeats; None while unavailable."""
        try:
            return environment_io_metrics(self.backend.metrics())
        except (OSError, RuntimeError, ValueError):
            return None


class EnvironmentImageRuntime:
    """Existing image API input adapter; pulls metadata/demanded bytes, not OCI layers."""
    dry_run = False
    materializes_rootfs = True
    pull_phase = "environment_resolve"
    # The gateway's pull attaches the root its create dispatches (chunk store
    # M2): a released image's manifest, and its old root, may be gone.
    pulls_environment_roots = True

    def __init__(self, image_store):
        self.image_store = image_store

    def pull(self, image, environment_root=None):
        from .sandbox import CommandResult
        self.image_store.warm(image, environment_root)
        return CommandResult(argv=("immutable-environment", "resolve", image), exit_code=0)

    def build(self, *args, **kwargs):
        raise ValueError("worker image builds are disabled; use the trusted builder")

    def push(self, *args, **kwargs):
        raise ValueError("immutable environment workers do not publish images")
