"""Quiescent blob collection for Distribution's filesystem storage driver.

The service coordinator stops Distribution, verifies its container has exited,
and holds the exclusive writer fence for this entire operation. Registry startup
holds the shared side of that fence. A maintenance lock alone is insufficient:
manifest PUT can pause indefinitely between validating blobs and committing.

Reference pruning remains online. Physical collection scans one stable tree;
there are no timing assumptions, incremental directory-mtime scans or writers
racing unlink/rename. Grace protects recent, not-yet-published uploads between
requests; it is not a substitute for the writer fence.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import shutil
import time
from typing import Any, Callable, Iterable, Iterator


_HEX = re.compile(r"[0-9a-f]{64}\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
SWEEP_SUFFIX = ".ucloud-sweep"
DEFAULT_SWEEP_BATCH = 2000
# Filesystem timestamps and the sweep clock may disagree slightly.
_CLOCK_SLACK_SECONDS = 1.0
_MAX_MANIFEST_BYTES = 4 * 1024 * 1024


class RegistrySweepAborted(RuntimeError):
    """Reachability is unknown; nothing may be deleted."""


@dataclass(frozen=True)
class RepositoryLinks:
    name: str
    path: Path
    revisions: dict[str, float]
    layers: dict[str, float]


@dataclass
class RegistrySweepResult:
    repositories: int = 0
    manifests: int = 0
    reachable_blobs: int = 0
    blobs: int = 0
    candidate_blobs: int = 0
    deleted_blobs: int = 0
    deleted_bytes: int = 0
    restored_blobs: int = 0
    removed_stale_links: int = 0
    restored_links: int = 0
    recovered_renames: int = 0
    skipped_recent: int = 0
    seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


class DistributionTree:
    """Read and minimally mutate one Distribution filesystem root."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.v2 = root / "docker" / "registry" / "v2"
        self.blobs = self.v2 / "blobs" / "sha256"
        self.repositories = self.v2 / "repositories"

    def blob_dir(self, digest: str) -> Path:
        hex_digest = digest.removeprefix("sha256:")
        return self.blobs / hex_digest[:2] / hex_digest

    def repository_roots(self) -> Iterator[tuple[str, Path]]:
        """Repository directories; path components starting with _ are metadata."""

        if not self.repositories.is_dir():
            return
        pending = [(self.repositories, "")]
        while pending:
            directory, prefix = pending.pop()
            try:
                entries = list(os.scandir(directory))
            except FileNotFoundError:
                continue
            names = {entry.name for entry in entries}
            if prefix and names & {"_manifests", "_layers", "_uploads"}:
                yield prefix, directory
            for entry in entries:
                if entry.name.startswith("_") or not entry.is_dir(follow_symlinks=False):
                    continue
                pending.append(
                    (Path(entry.path), f"{prefix}/{entry.name}" if prefix else entry.name)
                )

    def links(self, directory: Path, *, since: float | None = None) -> dict[str, float]:
        """``sha256/<hex>/link`` targets below ``directory`` and their mtimes."""

        sha_dir = directory / "sha256"
        found: dict[str, float] = {}
        try:
            entries = list(os.scandir(sha_dir))
        except FileNotFoundError:
            return found
        for entry in entries:
            if not _HEX.match(entry.name):
                continue
            try:
                mtime = os.stat(os.path.join(entry.path, "link")).st_mtime
            except FileNotFoundError:
                continue
            if since is None or mtime >= since - _CLOCK_SLACK_SECONDS:
                found["sha256:" + entry.name] = mtime
        return found

    def scan_repository(
        self, name: str, path: Path, *, since: float | None = None,
    ) -> RepositoryLinks:
        return RepositoryLinks(
            name=name,
            path=path,
            revisions=self.links(path / "_manifests" / "revisions", since=since),
            layers=self.links(path / "_layers", since=since),
        )

    def manifest_references(self, digest: str) -> tuple[set[str], set[str]] | None:
        """(child manifests, blobs) of a manifest; None when its data is gone."""

        data = self.blob_dir(digest) / "data"
        try:
            with data.open("rb") as handle:
                payload = handle.read(_MAX_MANIFEST_BYTES + 1)
        except FileNotFoundError:
            return None
        if len(payload) > _MAX_MANIFEST_BYTES:
            raise RegistrySweepAborted(f"manifest {digest} exceeds the parse limit")
        try:
            document = json.loads(payload)
        except ValueError as exc:
            raise RegistrySweepAborted(f"manifest {digest} is not JSON") from exc
        if not isinstance(document, dict):
            raise RegistrySweepAborted(f"manifest {digest} is not an object")
        if not any(key in document for key in ("config", "layers", "manifests", "fsLayers", "blobs")):
            raise RegistrySweepAborted(f"manifest {digest} has an unknown reference layout")

        def references(field, digest_key="digest"):
            values = document.get(field, [])
            if not isinstance(values, list):
                raise RegistrySweepAborted(f"manifest {digest} has invalid {field}")
            result = set()
            for item in values:
                if not isinstance(item, dict) or not _DIGEST.fullmatch(str(item.get(digest_key) or "")):
                    raise RegistrySweepAborted(f"manifest {digest} has an invalid {field} descriptor")
                result.add(item[digest_key])
            return result

        children: set[str] = set()
        blobs: set[str] = set()
        children |= references("manifests")
        blobs |= references("layers") | references("blobs") | references("fsLayers", "blobSum")
        for field, target in (("config", blobs), ("subject", children)):
            if field not in document:
                continue
            item = document[field]
            if not isinstance(item, dict) or not _DIGEST.fullmatch(str(item.get("digest") or "")):
                raise RegistrySweepAborted(f"manifest {digest} has an invalid {field} descriptor")
            target.add(item["digest"])
        return children, blobs

    def closure(self, manifests: Iterable[str]) -> set[str]:
        """Manifest digests plus everything they reference, transitively."""

        reachable: set[str] = set()
        pending = list(manifests)
        while pending:
            digest = pending.pop()
            if digest in reachable:
                continue
            reachable.add(digest)
            references = self.manifest_references(digest)
            if references is None:
                raise RegistrySweepAborted(f"referenced manifest {digest} has no data")
            children, blobs = references
            reachable |= blobs
            pending.extend(children - reachable)
        return reachable

    def blob_entries(self) -> Iterator[tuple[str, Path]]:
        try:
            prefixes = list(os.scandir(self.blobs))
        except FileNotFoundError:
            return
        for prefix in prefixes:
            if not prefix.is_dir(follow_symlinks=False):
                continue
            for entry in os.scandir(prefix.path):
                if _HEX.match(entry.name):
                    yield "sha256:" + entry.name, Path(entry.path)

    def renamed_entries(self) -> Iterator[tuple[str, Path]]:
        try:
            prefixes = list(os.scandir(self.blobs))
        except FileNotFoundError:
            return
        for prefix in prefixes:
            if not prefix.is_dir(follow_symlinks=False):
                continue
            for entry in os.scandir(prefix.path):
                name = entry.name.removesuffix(SWEEP_SUFFIX)
                if entry.name.endswith(SWEEP_SUFFIX) and _HEX.match(name):
                    yield "sha256:" + name, Path(entry.path)


def _mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return None


def _dir_size(path: Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except FileNotFoundError:
                pass
    return total


def _remove_link(link_dir: Path) -> bool:
    try:
        (link_dir / "link").unlink()
    except FileNotFoundError:
        return False
    try:
        link_dir.rmdir()
    except OSError:
        pass
    return True


def _restore_link(link_dir: Path, digest: str) -> None:
    link_dir.mkdir(parents=True, exist_ok=True)
    link = link_dir / "link"
    if not link.exists():
        link.write_text(digest, encoding="ascii")


def sweep_registry_blobs(
    root: Path,
    *,
    grace_seconds: float,
    writers_stopped: bool = False,
    batch_size: int = DEFAULT_SWEEP_BATCH,
    clock: Callable[[], float] = time.time,
) -> RegistrySweepResult:
    """Collect under the coordinator's exclusive writer fence, never online."""

    if not writers_stopped:
        raise RegistrySweepAborted("blob collection requires stopped registry writers and the exclusive writer fence")
    if grace_seconds < 0 or batch_size < 1:
        raise ValueError("invalid registry collection grace or batch size")
    started = clock()
    tree = DistributionTree(root)
    journal = SweepJournal(root / JOURNAL_NAME)
    result = RegistrySweepResult()
    cutoff = started - grace_seconds
    _recover_interrupted(tree, journal, result)
    repositories = [tree.scan_repository(name, path) for name, path in tree.repository_roots()]
    result.repositories = len(repositories)
    manifests = {digest for repo in repositories for digest in repo.revisions}
    result.manifests = len(manifests)
    # Resolve every repository before mutating anything. An unreadable/missing
    # manifest must abort the whole collection, not yield an incomplete closure.
    own = {repo.name: tree.closure(repo.revisions) for repo in repositories}
    reachable = set().union(*own.values()) if own else set()
    result.reachable_blobs = len(reachable)
    links_by_blob: dict[str, list[tuple[Path, float]]] = {}
    for repo in repositories:
        for digest, mtime in repo.layers.items():
            links_by_blob.setdefault(digest, []).append(
                (repo.path / "_layers" / "sha256" / digest.removeprefix("sha256:"), mtime)
            )
    candidates = []
    for digest, blob_dir in tree.blob_entries():
        result.blobs += 1
        if digest in reachable:
            continue
        data_mtime = _mtime(blob_dir / "data") or _mtime(blob_dir)
        links = links_by_blob.get(digest, ())
        if data_mtime is None or data_mtime >= cutoff or any(mtime >= cutoff for _, mtime in links):
            result.skipped_recent += 1
            continue
        candidates.append(digest)
    result.candidate_blobs = len(candidates)
    for start in range(0, len(candidates), batch_size):
        batch = candidates[start:start + batch_size]
        journal.write(blobs={digest: [str(path) for path, _ in links_by_blob.get(digest, ())]
                             for digest in batch})
        for digest in batch:
            blob_dir = tree.blob_dir(digest)
            for link_dir, _ in links_by_blob.get(digest, ()):
                _remove_link(link_dir)
            aside = blob_dir.with_name(blob_dir.name + SWEEP_SUFFIX)
            blob_dir.rename(aside)
            result.deleted_bytes += _dir_size(aside)
            shutil.rmtree(aside)
            result.deleted_blobs += 1
        journal.clear()
    # Links in a repository without a referencing manifest are also stale,
    # even if another repository keeps the blob alive. Keep recent uploads.
    stale = {}
    for repo in repositories:
        for digest, mtime in repo.layers.items():
            if mtime < cutoff and digest in reachable and digest not in own[repo.name]:
                path = repo.path / "_layers" / "sha256" / digest.removeprefix("sha256:")
                stale[str(path)] = digest
    if stale:
        journal.write(links=stale)
        for path in stale:
            result.removed_stale_links += _remove_link(Path(path))
        journal.clear()
    result.seconds = round(clock() - started, 3)
    return result


JOURNAL_NAME = "ucloud-sweep-journal.json"


class SweepJournal:
    """Blob renames and link removals of the batch in progress."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def write(
        self,
        *,
        blobs: dict[str, list[str]] | None = None,
        links: dict[str, str] | None = None,
    ) -> None:
        temporary = self.path.with_name(self.path.name + ".tmp")
        temporary.write_text(
            json.dumps({"blobs": blobs or {}, "links": links or {}}, sort_keys=True),
            encoding="utf-8",
        )
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)
        descriptor = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def read(self) -> dict[str, Any] | None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except ValueError as exc:
            raise RegistrySweepAborted("sweep journal is unreadable") from exc
        if not isinstance(raw, dict) or not isinstance(raw.get("blobs", {}), dict) or not isinstance(raw.get("links", {}), dict):
            raise RegistrySweepAborted("sweep journal has an invalid structure")
        return raw

    def clear(self) -> None:
        self.path.unlink(missing_ok=True)


def _recover_interrupted(
    tree: DistributionTree,
    journal: SweepJournal,
    result: RegistrySweepResult,
) -> None:
    """Recover incomplete collection, including journals from the old online sweep.

    Retain remaining blobs/links conservatively, then recompute reachability
    under the writer fence before collecting anything.
    """

    state = journal.read() or {}
    for digest, link_dirs in (state.get("blobs") or {}).items():
        blob_dir = tree.blob_dir(str(digest))
        aside = blob_dir.with_name(blob_dir.name + SWEEP_SUFFIX)
        # A blob the batch already deleted keeps no links.
        if not _DIGEST.match(str(digest)) or not (blob_dir.exists() or aside.exists()):
            continue
        for link_dir in link_dirs:
            _restore_link(Path(link_dir), digest)
    for link_dir, digest in (state.get("links") or {}).items():
        if _DIGEST.match(str(digest)):
            _restore_link(Path(link_dir), digest)
            result.restored_links += 1
    for digest, aside in list(tree.renamed_entries()):
        blob_dir = tree.blob_dir(digest)
        if blob_dir.exists():
            shutil.rmtree(aside, ignore_errors=True)
        else:
            aside.rename(blob_dir)
        result.recovered_renames += 1
    journal.clear()
