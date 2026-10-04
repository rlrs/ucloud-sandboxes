"""Volume-free builds (docs/chunk-store-m2-plan.md §5.4).

A build whose Dockerfile or build arguments name an image whose OCI manifest
``chunk-migrate release-oci`` deleted gets that image back as a BuildKit named
context: ``--build-context <reference>=docker-image://<regenerated copy>``.
The context archive stays byte-identical, so prepared decisions, cache
affinity and build identities do not move. The copy is another OCI image (one
layer, new digests): its tree is the verified one, its config the original.

Only an image with a verified regeneration receipt is regenerated, and the
regenerated layer must reproduce the receipt's diff ID. The gateway does not
regenerate inline: it starts ``chunk-migrate regenerate`` (one per image, a
bounded number per host) and answers a retryable 503 until the copy exists.
"""
from contextlib import contextmanager
import fcntl
import gzip
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import threading
import time

from ..managed_registry import RegistryRequestError, manifest_digest_from_image_ref, \
    registry_repository_tag_from_image_ref

# Outside ``ucloud-managed/``: no LRU eviction, and the hourly prune's age rule
# drops a copy 30 days after its last build (each build leases and touches it).
REGENERATED_REPOSITORY = "ucloud-regenerated"
# Concurrent regenerations per gateway host: each gzips a whole image and
# needs about three device sizes of scratch on the state disk.
REGENERATION_SLOTS = 2
SCRATCH_RESERVE_BYTES = 16 * 1024 ** 3
# A failed regeneration answers with its error this long before a build retries it.
FAILURE_BACKOFF_SECONDS = 600
MAX_DOCKERFILE_BYTES = 1024 * 1024


class BaseRegenerating(RuntimeError):
    """Retryable: the named images' copies are being regenerated."""


class BaseReleased(ValueError):
    """Not regenerable: no verified receipt, or its last regeneration failed."""


def regenerated_tag(repository, digest):
    return "r-" + hashlib.sha256(f"{repository}@{digest}".encode()).hexdigest()[:40]


def regeneration_root(state_dir):
    return Path(state_dir) / "base-regeneration"


def _try_lock(path):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(descriptor)
        return None
    return descriptor


@contextmanager
def regeneration_claim(work_root, repository, digest, *, slots=REGENERATION_SLOTS, poll=5.0):
    """Yields False when another process regenerates this image; otherwise
    waits for one of ``slots`` and yields True. flock: replicas and restarts
    share it, and a dead holder releases it."""
    held = _try_lock(Path(work_root) / "locks" / (regenerated_tag(repository, digest) + ".lock"))
    if held is None:
        yield False
        return
    slot = None
    try:
        while slot is None:
            slot = next(filter(None, (_try_lock(Path(work_root) / "locks" / f"slot-{index}.lock")
                                      for index in range(slots))), None)
            if slot is None:
                time.sleep(poll)
        yield True
    finally:
        for descriptor in (slot, held):
            if descriptor is not None:
                os.close(descriptor)


def regenerating(work_root, repository, digest):
    descriptor = _try_lock(Path(work_root) / "locks" / (regenerated_tag(repository, digest) + ".lock"))
    if descriptor is not None:
        os.close(descriptor)
    return descriptor is None


def context_dockerfile(store, digest, name):
    """The Dockerfile's text in a stored build context (tar.gz), or ""."""
    from ..images import _validate_context_member
    wanted = os.path.normpath(name)
    with store.open(digest) as stream, gzip.GzipFile(fileobj=stream) as zipped, \
            tarfile.open(fileobj=zipped, mode="r|") as archive:
        for member in archive:
            _validate_context_member(member)
            if member.isfile() and os.path.normpath(member.name) == wanted:
                if member.size > MAX_DOCKERFILE_BYTES:
                    raise ValueError("the build's Dockerfile is too large")
                return archive.extractfile(member).read().decode("utf-8", "surrogateescape")
    return ""


def spawner(config_path, work_root):
    """Start ``chunk-migrate regenerate`` for one image, detached from the request."""
    from ..chunk_migrate import CLI

    def spawn(repository, digest):
        logs = Path(work_root) / "logs"
        logs.mkdir(mode=0o700, parents=True, exist_ok=True)
        with open(logs / (regenerated_tag(repository, digest) + ".log"), "ab") as log:
            process = subprocess.Popen([sys.executable, "-c", CLI, "chunk-migrate", "regenerate", "--config",
                                        str(config_path), "--image", f"{repository}@{digest}"],
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        threading.Thread(target=process.wait, daemon=True).start()  # Reaped, never awaited.
    return spawn


class BaseRegeneration:
    """The named contexts a build needs for the OCI-released images it names."""

    def __init__(self, roots, client, *, registry_hosts, worker_host, work_root, spawn):
        self.roots, self.client, self.worker_host = roots, client, worker_host
        self.work_root, self.spawn = Path(work_root), spawn
        hosts = "|".join(re.escape(host) for host in sorted(set(registry_hosts)) if host)
        # A whole reference on our registry; a released one is rare, so every
        # match is checked rather than parsing FROM, COPY --from and ARG forms.
        self.pattern = re.compile(rf"(?<![\w.:/@-])(?:{hosts})/[a-z0-9]+(?:[._/-][a-z0-9]+)*"
                                  r"(?::[\w][\w.-]{0,127})?(?:@sha256:[a-f0-9]{64})?")

    def contexts(self, texts):
        """({name: docker-image://copy}, [copies]) for each OCI-released image
        the texts name (an iterable, read only once some image has a receipt).
        Raises BaseRegenerating while a copy is missing (its regeneration is
        started) and BaseReleased when there can be none."""
        regenerable = self.roots.regenerable()
        if not regenerable:
            return {}, []
        contexts, copies, pending = {}, [], []
        for name in sorted({match[0] for text in texts for match in self.pattern.finditer(text)}):
            repository, tag = registry_repository_tag_from_image_ref(name) or ("", "")
            reference = manifest_digest_from_image_ref(name) or tag
            digest = repository and self.roots.released_digest(repository, reference)
            if not digest or self._present(repository, reference):  # The registry answers first.
                continue
            if (repository, digest) not in regenerable:
                raise BaseReleased(f"{name}: its OCI was released without a verified regeneration")
            copy = self._copy(repository, digest)
            if copy is None:
                pending.append(name)
                continue
            copies.append(copy)
            for key in {name, name[:-len(":latest")] if name.endswith(":latest") else name}:
                contexts[key] = "docker-image://" + copy  # BuildKit names an untagged FROM without ":latest".
        if pending:
            raise BaseRegenerating("regenerating released build bases: " + ", ".join(pending))
        return contexts, copies

    def _present(self, repository, reference):
        try:
            return bool(self.client.manifest_digest(repository, reference))
        except RegistryRequestError as exc:
            if exc.status_code != 404:
                raise
            return False

    def _copy(self, repository, digest):
        row, tag = self.roots.regeneration(repository, digest), regenerated_tag(repository, digest)
        if row["regenerated_digest"]:
            try:
                if self.client.manifest_digest(REGENERATED_REPOSITORY, tag) == row["regenerated_digest"]:
                    return f"{self.worker_host}/{REGENERATED_REPOSITORY}:{tag}@{row['regenerated_digest']}"
            except RegistryRequestError as exc:
                if exc.status_code != 404:  # A pruned copy is regenerated again.
                    raise
        if row["error"] and time.time() - row["attempted"] < FAILURE_BACKOFF_SECONDS:
            raise BaseReleased(f"{repository}@{digest}: regeneration failed: {row['error']}")
        if not regenerating(self.work_root, repository, digest):
            self.spawn(repository, digest)
        return None


def from_deployment(config, config_path):
    """The gateway's regeneration, with ``immutable_environments.regenerate_bases`` on."""
    selected = config.immutable_environments
    if selected is None or not selected.regenerate_bases:
        return None
    from urllib.parse import urlparse
    from ..managed_registry import RegistryClient
    from .image_roots import ImageRootsStore, roots_path
    work_root = regeneration_root(config.control_state_file().parent)
    return BaseRegeneration(ImageRootsStore(roots_path(config.image_file())), RegistryClient(config.registry_url),
                            registry_hosts=(urlparse(config.registry_url).netloc,
                                            urlparse(config.registry_worker_url or "").netloc),
                            worker_host=urlparse(config.registry_worker_url or config.registry_url).netloc,
                            work_root=work_root, spawn=spawner(config_path, work_root))
