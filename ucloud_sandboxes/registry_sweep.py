"""Online blob sweep for Docker Distribution's filesystem storage driver.

Distribution's own ``garbage-collect`` must run with the registry stopped or
read-only; on a volume with ~160k blobs that blocks pushes for tens of
minutes. This sweep reclaims the same space while the registry serves reads
and writes, using only the on-disk layout the filesystem driver defines::

    blobs/sha256/<2 hex>/<hex>/data                     blob content
    repositories/<name>/_manifests/revisions/sha256/<hex>/link
    repositories/<name>/_manifests/tags/<tag>/current/link
    repositories/<name>/_layers/sha256/<hex>/link       repository blob access

Reachability. Every revision link names a manifest a repository still holds.
The reachable set is those manifest blobs plus their config, layers, and (for
indexes) child manifests. Registry deletions (retention, eviction) remove
revision links through the HTTP API, so their blobs become unreachable.

Candidates. An unreachable blob is swept only when its data and every
repository link to it are older than ``grace_seconds``, which exceeds any
push: blobs of an in-flight push are linked and written within it.

Race analysis. The registry must run without its blob descriptor cache
(``systemd.registry_process_environment`` disables it for this driver), so
the filesystem is the only truth. A push can make a swept blob reachable
again only through a manifest PUT, and Distribution verifies every layer and
config of a PUT through that repository's link and the blob data; child
manifests through their revision links. Each batch therefore:

1. unlinks the candidates' repository links and renames each blob directory
   aside (``<hex>.ucloud-sweep``); from then on a PUT that references the
   blob fails verification (the push retries the upload) instead of
   committing a dangling manifest, and a fresh upload writes a new blob
   directory;
2. waits ``settle_seconds``, longer than one PUT handler takes between its
   verification and its revision link write;
3. rescans links created since the sweep started. A manifest verified
   before step 1 had a link then, and its revision link now exists; either
   one restores the blob and its links. Otherwise the renamed directory is
   deleted.

A PUT that races step 1 fails with BLOB_UNKNOWN rather than corrupting the
registry. Stale repository links (a repository's link to a blob none of its
own manifests reference) are removed with the same unlink, settle, recheck,
restore sequence so a later push re-uploads rather than trusting them.
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
DEFAULT_SETTLE_SECONDS = 2.0
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
        if since is not None:
            try:
                if sha_dir.stat().st_mtime < since - _CLOCK_SLACK_SECONDS:
                    return {}
            except FileNotFoundError:
                return {}
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
        children: set[str] = set()
        blobs: set[str] = set()
        for item in document.get("manifests") or ():
            if isinstance(item, dict) and _DIGEST.match(str(item.get("digest") or "")):
                children.add(item["digest"])
        descriptors = [document.get("config"), document.get("subject")]
        descriptors.extend(document.get("layers") or ())
        descriptors.extend(document.get("blobs") or ())
        for item in descriptors:
            if isinstance(item, dict) and _DIGEST.match(str(item.get("digest") or "")):
                blobs.add(item["digest"])
        for item in document.get("fsLayers") or ():  # Docker schema 1
            if isinstance(item, dict) and _DIGEST.match(str(item.get("blobSum") or "")):
                blobs.add(item["blobSum"])
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
                continue
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
    batch_size: int = DEFAULT_SWEEP_BATCH,
    settle_seconds: float = DEFAULT_SETTLE_SECONDS,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    before_batch: Callable[[list[str]], None] | None = None,
) -> RegistrySweepResult:
    """Delete unreachable blobs and stale links while the registry runs."""

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
    reachable = tree.closure(manifests)
    result.reachable_blobs = len(reachable)
    links_by_blob: dict[str, list[tuple[Path, float]]] = {}
    for repo in repositories:
        for digest, mtime in repo.layers.items():
            links_by_blob.setdefault(digest, []).append(
                (repo.path / "_layers" / "sha256" / digest.removeprefix("sha256:"), mtime)
            )

    candidates: list[str] = []
    for digest, blob_dir in tree.blob_entries():
        result.blobs += 1
        if digest in reachable:
            continue
        data_mtime = _mtime(blob_dir / "data") or _mtime(blob_dir)
        links = links_by_blob.get(digest, ())
        if data_mtime is None or data_mtime >= cutoff or any(
            mtime >= cutoff for _path, mtime in links
        ):
            result.skipped_recent += 1
            continue
        candidates.append(digest)
    result.candidate_blobs = len(candidates)

    def fresh_references() -> tuple[set[str], set[str]]:
        """Blobs referenced or linked by anything written since the scan."""

        new_revisions: set[str] = set()
        new_links: set[str] = set()
        for name, path in tree.repository_roots():
            repo = tree.scan_repository(name, path, since=started)
            new_revisions |= set(repo.revisions)
            new_links |= set(repo.layers)
        return tree.closure(new_revisions), new_links

    step = max(1, batch_size)
    for start in range(0, len(candidates), step):
        batch = candidates[start:start + step]
        if before_batch is not None:
            before_batch(batch)
        referenced, linked = fresh_references()
        batch = [
            digest
            for digest in batch
            if digest not in referenced and digest not in linked
        ]
        # Journal first: a crash after any unlink or rename must be undone.
        journal.write(
            blobs={
                digest: [str(path) for path, _mtime_value in links_by_blob.get(digest, ())]
                for digest in batch
            }
        )
        renamed: list[tuple[str, Path, Path, list[Path]]] = []
        for digest in batch:
            blob_dir = tree.blob_dir(digest)
            data_mtime = _mtime(blob_dir / "data") or _mtime(blob_dir)
            if data_mtime is None or data_mtime >= cutoff:
                continue
            removed_links = [
                link_dir
                for link_dir, _mtime_value in links_by_blob.get(digest, ())
                if _remove_link(link_dir)
            ]
            aside = blob_dir.with_name(blob_dir.name + SWEEP_SUFFIX)
            try:
                blob_dir.rename(aside)
            except FileNotFoundError:
                for link_dir in removed_links:
                    _restore_link(link_dir, digest)
                continue
            renamed.append((digest, blob_dir, aside, removed_links))
        if not renamed:
            journal.clear()
            continue
        sleep(settle_seconds)
        referenced, linked = fresh_references()
        for digest, blob_dir, aside, removed_links in renamed:
            if digest in referenced or digest in linked:
                # A push referenced the blob before its links were removed.
                if not blob_dir.exists():
                    aside.rename(blob_dir)
                else:
                    shutil.rmtree(aside, ignore_errors=True)
                for link_dir in removed_links:
                    _restore_link(link_dir, digest)
                result.restored_blobs += 1
                continue
            size = _dir_size(aside)
            shutil.rmtree(aside)
            result.deleted_blobs += 1
            result.deleted_bytes += size
        journal.clear()

    _remove_stale_links(
        tree, repositories, reachable=reachable, cutoff=cutoff, started=started,
        settle_seconds=settle_seconds, sleep=sleep, result=result, journal=journal,
    )
    result.seconds = round(clock() - started, 3)
    return result


def _remove_stale_links(
    tree: DistributionTree,
    repositories: list[RepositoryLinks],
    *,
    reachable: set[str],
    cutoff: float,
    started: float,
    settle_seconds: float,
    sleep: Callable[[float], None],
    result: RegistrySweepResult,
    journal: "SweepJournal",
) -> None:
    """Drop repository links to blobs that no manifest of the repository uses.

    Links to unreachable blobs were handled with their blobs. A link removed
    here is restored if a manifest written meanwhile in that repository
    references the blob, because Distribution serves a blob to a repository
    only through its link.
    """

    selected: list[tuple[RepositoryLinks, str, Path]] = []
    for repo in repositories:
        stale = [
            digest
            for digest, mtime in repo.layers.items()
            if mtime < cutoff and digest in reachable
        ]
        if not stale:
            continue
        own = tree.closure(repo.revisions)
        selected.extend(
            (repo, digest, repo.path / "_layers" / "sha256" / digest.removeprefix("sha256:"))
            for digest in stale
            if digest not in own
        )
    if not selected:
        return
    journal.write(links={str(link_dir): digest for _repo, digest, link_dir in selected})
    removed = [item for item in selected if _remove_link(item[2])]
    if not removed:
        journal.clear()
        return
    sleep(settle_seconds)
    fresh: dict[Path, set[str]] = {}
    for repo, digest, link_dir in removed:
        if repo.path not in fresh:
            current = tree.scan_repository(repo.name, repo.path, since=started)
            fresh[repo.path] = tree.closure(current.revisions) | set(current.layers)
        if digest in fresh[repo.path]:
            _restore_link(link_dir, digest)
            result.restored_links += 1
        else:
            result.removed_stale_links += 1
    journal.clear()


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
        os.replace(temporary, self.path)

    def read(self) -> dict[str, Any] | None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except ValueError as exc:
            raise RegistrySweepAborted("sweep journal is unreadable") from exc
        return raw if isinstance(raw, dict) else None

    def clear(self) -> None:
        self.path.unlink(missing_ok=True)


def _recover_interrupted(
    tree: DistributionTree,
    journal: SweepJournal,
    result: RegistrySweepResult,
) -> None:
    """Undo a batch that died between its first unlink and its recheck.

    Without the settle-and-recheck evidence nothing in it is proven dead:
    renamed blobs and every journaled link go back, and a later sweep
    reconsiders them from scratch.
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
