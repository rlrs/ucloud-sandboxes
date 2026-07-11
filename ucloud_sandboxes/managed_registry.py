from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import fcntl
import json
import math
import os
from pathlib import Path
import re
from threading import RLock, get_ident
import time
from typing import Any, Callable, Iterable, Iterator, Mapping
from urllib import error, request
from urllib.parse import quote, urlencode, urlparse

from .models import parse_iso_datetime


MANIFEST_ACCEPT = ", ".join(
    (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)
_REGISTRY_FILE_LOCKS_GUARD = RLock()
_REGISTRY_FILE_LOCKS: dict[Path, RLock] = {}
MAX_REGISTRY_LEASE_TTL_SECONDS = 24 * 60 * 60
MAX_REGISTRY_PAGINATION_PAGES = 10_000
MAX_REGISTRY_JSON_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_REGISTRY_ERROR_PREVIEW_BYTES = 64 * 1024
_MANIFEST_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_DIGEST_PROTECTION_TAG_RE = re.compile(r"^ucloud-digest-sha256-[0-9a-f]{64}$")


@dataclass(frozen=True)
class RegistryTag:
    repository: str
    tag: str
    digest: str
    created_at: str = ""
    last_used_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "repository": self.repository,
            "tag": self.tag,
            "digest": self.digest,
            "created_at": self.created_at,
            "last_used_at": self.last_used_at,
        }


@dataclass(frozen=True)
class RegistryImageUsage:
    image_ref: str
    repository: str
    tag: str
    last_used_at: str

    @classmethod
    def from_dict(cls, raw: object) -> "RegistryImageUsage | None":
        if not isinstance(raw, dict):
            return None
        image_ref = str(raw.get("image_ref") or raw.get("imageRef") or "")
        repository = str(raw.get("repository") or "")
        tag = str(raw.get("tag") or "")
        last_used_at = str(raw.get("last_used_at") or raw.get("lastUsedAt") or "")
        if not image_ref or not repository or not tag or not last_used_at:
            return None
        return cls(
            image_ref=image_ref,
            repository=repository,
            tag=tag,
            last_used_at=last_used_at,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "image_ref": self.image_ref,
            "repository": self.repository,
            "tag": self.tag,
            "last_used_at": self.last_used_at,
        }


@dataclass(frozen=True)
class RegistryImageLease:
    repository: str
    tag: str
    owner: str
    acquired_at: str
    renewed_at: str
    expires_at: str
    digest: str = ""

    @classmethod
    def from_dict(cls, raw: object) -> "RegistryImageLease | None":
        if not isinstance(raw, dict):
            return None
        repository = str(raw.get("repository") or "").strip()
        tag = str(raw.get("tag") or "").strip()
        owner = str(raw.get("owner") or "").strip()
        acquired_at = str(raw.get("acquired_at") or raw.get("acquiredAt") or "")
        renewed_at = str(raw.get("renewed_at") or raw.get("renewedAt") or "")
        expires_at = str(raw.get("expires_at") or raw.get("expiresAt") or "")
        raw_digest = str(raw.get("digest") or raw.get("manifest_digest") or "")
        digest = normalize_manifest_digest(raw_digest)
        persistent = raw.get("persistent", False)
        if not repository or not tag or not owner:
            return None
        if (
            parse_iso_datetime(acquired_at) is None
            or parse_iso_datetime(renewed_at) is None
            or (
                not (persistent is True and not expires_at)
                and parse_iso_datetime(expires_at) is None
            )
        ):
            return None
        if raw_digest and not digest:
            return None
        return cls(
            repository=repository,
            tag=tag,
            owner=owner,
            acquired_at=acquired_at,
            renewed_at=renewed_at,
            expires_at=expires_at,
            digest=digest,
        )

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.repository, self.tag, self.owner)

    def is_active(self, now: datetime) -> bool:
        if not self.expires_at:
            return True
        expires_at = parse_iso_datetime(self.expires_at)
        return expires_at is not None and expires_at > _as_utc(now)

    def to_dict(self) -> dict[str, str]:
        return {
            "repository": self.repository,
            "tag": self.tag,
            "owner": self.owner,
            "acquired_at": self.acquired_at,
            "renewed_at": self.renewed_at,
            "expires_at": self.expires_at,
            "persistent": not self.expires_at,
            "digest": self.digest,
        }


@dataclass(frozen=True)
class RegistryUsageSnapshot:
    generation: int
    records: dict[tuple[str, str], RegistryImageUsage]
    leases: dict[tuple[str, str, str], RegistryImageLease] = field(default_factory=dict)

    def active_lease_tags(self, *, now: datetime | None = None) -> set[tuple[str, str]]:
        reference = _as_utc(now or datetime.now(timezone.utc))
        return {
            (lease.repository, lease.tag)
            for lease in self.leases.values()
            if not lease.digest and lease.is_active(reference)
        }

    def active_lease_digests(
        self,
        *,
        now: datetime | None = None,
    ) -> set[tuple[str, str]]:
        reference = _as_utc(now or datetime.now(timezone.utc))
        return {
            (lease.repository, lease.digest)
            for lease in self.leases.values()
            if lease.digest and lease.is_active(reference)
        }


class RegistryUsageGenerationChanged(RuntimeError):
    pass


class RegistryImageLeaseNotFound(KeyError):
    pass


class RegistryMaintenanceBusy(RuntimeError):
    pass


class RegistryRequestError(ValueError):
    def __init__(
        self,
        status_code: int,
        method: str,
        path: str,
        body: str,
    ) -> None:
        super().__init__(
            f"registry request failed ({status_code}) {method} {path}: {body}"
        )
        self.status_code = status_code
        self.method = method
        self.path = path
        self.body = body


class RegistryClient:
    def __init__(self, base_url: str, *, timeout_seconds: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def catalog(self) -> list[str]:
        found: list[str] = []
        path = "/v2/_catalog?" + urlencode({"n": 1000})
        visited: set[str] = set()
        while path:
            if path in visited:
                raise ValueError("registry catalog returned a repeated pagination link")
            if len(visited) >= MAX_REGISTRY_PAGINATION_PAGES:
                raise ValueError("registry catalog exceeded the pagination page limit")
            visited.add(path)
            payload, headers = self._json_request(path)
            repositories = payload.get("repositories")
            if isinstance(repositories, list):
                found.extend(item for item in repositories if isinstance(item, str))
            path = _next_link_path(headers.get("Link"))
        return list(dict.fromkeys(found))

    def tags(self, repository: str) -> list[str]:
        found: list[str] = []
        path = f"/v2/{_quote_repository(repository)}/tags/list?" + urlencode(
            {"n": 1000}
        )
        visited: set[str] = set()
        while path:
            if path in visited:
                raise ValueError("registry tags returned a repeated pagination link")
            if len(visited) >= MAX_REGISTRY_PAGINATION_PAGES:
                raise ValueError("registry tags exceeded the pagination page limit")
            visited.add(path)
            payload, headers = self._json_request(path)
            tags = payload.get("tags")
            if isinstance(tags, list):
                found.extend(item for item in tags if isinstance(item, str))
            path = _next_link_path(headers.get("Link"))
        return list(dict.fromkeys(found))

    def tag_record(self, repository: str, tag: str) -> RegistryTag | None:
        digest = self.manifest_digest(repository, tag)
        if not digest:
            return None
        return RegistryTag(
            repository=repository,
            tag=tag,
            digest=digest,
            created_at=self.created_at(repository, tag),
        )

    def tag_exists(self, repository: str, tag: str) -> bool:
        try:
            return bool(self.manifest_digest(repository, tag))
        except RegistryRequestError as exc:
            if exc.status_code == 404:
                return False
            raise

    def manifest_digest(self, repository: str, tag: str) -> str:
        path = f"/v2/{_quote_repository(repository)}/manifests/{quote(tag, safe='')}"
        response = self._request(
            path, method="HEAD", headers={"Accept": MANIFEST_ACCEPT}
        )
        try:
            digest = response.headers.get("Docker-Content-Digest")
        finally:
            response.close()
        if digest:
            return digest
        _body, headers = self._json_request(path, headers={"Accept": MANIFEST_ACCEPT})
        return headers.get("Docker-Content-Digest", "")

    def ensure_digest_protection_tag(self, repository: str, digest: str) -> str:
        """Ensure an immutable tag keeps ``digest`` reachable by registry GC."""

        normalized_digest = _validate_lease_digest(digest)
        protection_tag = digest_protection_tag(normalized_digest)
        try:
            protected_digest = normalize_manifest_digest(
                self.manifest_digest(repository, protection_tag)
            )
        except RegistryRequestError as exc:
            if exc.status_code != 404:
                raise
            protected_digest = ""
        if protected_digest:
            if protected_digest != normalized_digest:
                raise ValueError(
                    "registry digest protection tag points to a different manifest"
                )
            return protection_tag

        source_path = (
            f"/v2/{_quote_repository(repository)}/manifests/"
            f"{quote(normalized_digest, safe=':')}"
        )
        response = self._request(
            source_path,
            headers={"Accept": MANIFEST_ACCEPT},
        )
        try:
            manifest = response.read(MAX_REGISTRY_JSON_RESPONSE_BYTES + 1)
            content_type = str(response.headers.get("Content-Type") or "").strip()
        finally:
            response.close()
        if len(manifest) > MAX_REGISTRY_JSON_RESPONSE_BYTES:
            raise ValueError("registry manifest is too large to protect")
        if not manifest or not content_type:
            raise ValueError("registry returned an empty or untyped manifest")

        target_path = (
            f"/v2/{_quote_repository(repository)}/manifests/"
            f"{quote(protection_tag, safe='')}"
        )
        response = self._request(
            target_path,
            method="PUT",
            headers={"Content-Type": content_type},
            data=manifest,
        )
        response.close()
        stored_digest = normalize_manifest_digest(
            self.manifest_digest(repository, protection_tag)
        )
        if stored_digest != normalized_digest:
            raise ValueError("registry did not persist the digest protection tag")
        return protection_tag

    def created_at(self, repository: str, tag: str) -> str:
        try:
            manifest, _headers = self._json_request(
                f"/v2/{_quote_repository(repository)}/manifests/{quote(tag, safe='')}",
                headers={"Accept": MANIFEST_ACCEPT},
            )
            config = manifest.get("config")
            digest = config.get("digest") if isinstance(config, dict) else ""
            if not isinstance(digest, str) or not digest:
                return ""
            blob, _blob_headers = self._json_request(
                f"/v2/{_quote_repository(repository)}/blobs/{quote(digest, safe=':')}"
            )
            created = blob.get("created")
            return created if isinstance(created, str) else ""
        except (OSError, ValueError):
            return ""

    def delete_manifest(self, repository: str, digest: str) -> None:
        response = self._request(
            f"/v2/{_quote_repository(repository)}/manifests/{quote(digest, safe=':')}",
            method="DELETE",
        )
        response.close()

    def _json_request(
        self,
        path: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any], Any]:
        response = self._request(path, headers=headers)
        try:
            body = response.read(MAX_REGISTRY_JSON_RESPONSE_BYTES + 1)
            response_headers = dict(response.headers.items())
        finally:
            response.close()
        if len(body) > MAX_REGISTRY_JSON_RESPONSE_BYTES:
            raise ValueError(f"registry response is too large for {path}")
        payload = json.loads(body.decode("utf-8")) if body else {}
        if not isinstance(payload, dict):
            raise ValueError(f"registry returned non-object JSON for {path}")
        return payload, response_headers

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        data: bytes | None = None,
    ) -> Any:
        req = request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers=headers or {},
        )
        try:
            return request.urlopen(req, timeout=self.timeout_seconds)
        except error.HTTPError as exc:
            try:
                raw = exc.read(MAX_REGISTRY_ERROR_PREVIEW_BYTES + 1)
            finally:
                exc.close()
            if len(raw) > MAX_REGISTRY_ERROR_PREVIEW_BYTES:
                raw = raw[:MAX_REGISTRY_ERROR_PREVIEW_BYTES] + b"...<truncated>"
            body = raw.decode("utf-8", errors="replace")
            raise RegistryRequestError(exc.code, method, path, body) from exc


class RegistryUsageStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[tuple[str, str], RegistryImageUsage]:
        return self.snapshot().records

    def snapshot(self, *, now: datetime | None = None) -> RegistryUsageSnapshot:
        with _registry_file_lock(self.path):
            return self._prune_expired_leases_unlocked(
                self._load_snapshot_unlocked(),
                now=_as_utc(now or datetime.now(timezone.utc)),
            )

    def save(
        self,
        records: dict[tuple[str, str], RegistryImageUsage],
        *,
        expected_generation: int | None = None,
    ) -> int:
        with _registry_file_lock(self.path):
            current = self._prune_expired_leases_unlocked(
                self._load_snapshot_unlocked(),
                now=datetime.now(timezone.utc),
            )
            if (
                expected_generation is not None
                and current.generation != expected_generation
            ):
                raise RegistryUsageGenerationChanged(
                    "registry usage changed while maintenance was planned"
                )
            generation = current.generation + 1
            self._save_unlocked(
                records,
                current.leases,
                generation=generation,
            )
            return generation

    def assert_generation(self, expected_generation: int) -> None:
        actual = self.snapshot().generation
        if actual != expected_generation:
            raise RegistryUsageGenerationChanged(
                f"registry usage generation changed from {expected_generation} to {actual}"
            )

    def touch_image(
        self,
        image_ref: str,
        *,
        when: datetime | None = None,
    ) -> RegistryImageUsage | None:
        records = self.touch_images((image_ref,), when=when)
        return records[0] if records else None

    def touch_images(
        self,
        image_refs: Iterable[str],
        *,
        when: datetime | None = None,
    ) -> tuple[RegistryImageUsage, ...]:
        timestamp = when or datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        usage_records: list[RegistryImageUsage] = []
        for image_ref in image_refs:
            parsed = registry_repository_tag_from_image_ref(image_ref)
            if parsed is None:
                continue
            repository, tag = parsed
            usage_records.append(
                RegistryImageUsage(
                    image_ref=image_ref,
                    repository=repository,
                    tag=tag,
                    last_used_at=timestamp.astimezone(timezone.utc).isoformat(),
                )
            )
        if not usage_records:
            return ()
        with _registry_file_lock(self.path):
            snapshot = self._prune_expired_leases_unlocked(
                self._load_snapshot_unlocked(),
                now=timestamp,
            )
            records = dict(snapshot.records)
            for record in usage_records:
                records[(record.repository, record.tag)] = record
            self._save_unlocked(
                records,
                snapshot.leases,
                generation=snapshot.generation + 1,
            )
        return tuple(usage_records)

    def acquire_lease(
        self,
        repository: str,
        tag: str,
        owner: str,
        *,
        ttl_seconds: float,
        digest: str = "",
        now: datetime | None = None,
    ) -> RegistryImageLease:
        repository, tag, owner = _validate_lease_identity(repository, tag, owner)
        ttl = _validate_lease_ttl(ttl_seconds)
        normalized_digest = _validate_lease_digest(digest)
        timestamp = _as_utc(now or datetime.now(timezone.utc))
        with _registry_file_lock(self.path):
            snapshot = self._prune_expired_leases_unlocked(
                self._load_snapshot_unlocked(),
                now=timestamp,
            )
            key = (repository, tag, owner)
            previous = snapshot.leases.get(key)
            lease = RegistryImageLease(
                repository=repository,
                tag=tag,
                owner=owner,
                acquired_at=(
                    previous.acquired_at
                    if previous is not None
                    else timestamp.isoformat()
                ),
                renewed_at=timestamp.isoformat(),
                expires_at=(timestamp + timedelta(seconds=ttl)).isoformat(),
                digest=(
                    normalized_digest
                    or (previous.digest if previous is not None else "")
                ),
            )
            leases = dict(snapshot.leases)
            leases[key] = lease
            self._save_unlocked(
                snapshot.records,
                leases,
                generation=snapshot.generation + 1,
            )
            return lease

    def acquire_reference(
        self,
        repository: str,
        tag: str,
        owner: str,
        *,
        digest: str = "",
        now: datetime | None = None,
    ) -> RegistryImageLease:
        """Persist a non-expiring reference to an actively used image tag.

        Routes and accepted builds are durable facts rather than liveness
        leases. A leaked reference is conservative and may be reconciled
        explicitly; it must never disappear merely because a controller was
        unavailable for a renewal interval.
        """

        repository, tag, owner = _validate_lease_identity(repository, tag, owner)
        normalized_digest = _validate_lease_digest(digest)
        timestamp = _as_utc(now or datetime.now(timezone.utc))
        with _registry_file_lock(self.path):
            snapshot = self._prune_expired_leases_unlocked(
                self._load_snapshot_unlocked(),
                now=timestamp,
            )
            key = (repository, tag, owner)
            previous = snapshot.leases.get(key)
            reference = RegistryImageLease(
                repository=repository,
                tag=tag,
                owner=owner,
                acquired_at=(
                    previous.acquired_at
                    if previous is not None
                    else timestamp.isoformat()
                ),
                renewed_at=timestamp.isoformat(),
                expires_at="",
                digest=(
                    normalized_digest
                    or (previous.digest if previous is not None else "")
                ),
            )
            leases = dict(snapshot.leases)
            leases[key] = reference
            self._save_unlocked(
                snapshot.records,
                leases,
                generation=snapshot.generation + 1,
            )
            return reference

    def renew_lease(
        self,
        repository: str,
        tag: str,
        owner: str,
        *,
        ttl_seconds: float,
        digest: str = "",
        now: datetime | None = None,
    ) -> RegistryImageLease:
        repository, tag, owner = _validate_lease_identity(repository, tag, owner)
        ttl = _validate_lease_ttl(ttl_seconds)
        normalized_digest = _validate_lease_digest(digest)
        timestamp = _as_utc(now or datetime.now(timezone.utc))
        with _registry_file_lock(self.path):
            snapshot = self._prune_expired_leases_unlocked(
                self._load_snapshot_unlocked(),
                now=timestamp,
            )
            key = (repository, tag, owner)
            previous = snapshot.leases.get(key)
            if previous is None:
                raise RegistryImageLeaseNotFound(key)
            lease = RegistryImageLease(
                repository=repository,
                tag=tag,
                owner=owner,
                acquired_at=previous.acquired_at,
                renewed_at=timestamp.isoformat(),
                expires_at=(timestamp + timedelta(seconds=ttl)).isoformat(),
                digest=normalized_digest or previous.digest,
            )
            leases = dict(snapshot.leases)
            leases[key] = lease
            self._save_unlocked(
                snapshot.records,
                leases,
                generation=snapshot.generation + 1,
            )
            return lease

    def release_lease(
        self,
        repository: str,
        tag: str,
        owner: str,
        *,
        now: datetime | None = None,
    ) -> bool:
        repository, tag, owner = _validate_lease_identity(repository, tag, owner)
        timestamp = _as_utc(now or datetime.now(timezone.utc))
        with _registry_file_lock(self.path):
            snapshot = self._prune_expired_leases_unlocked(
                self._load_snapshot_unlocked(),
                now=timestamp,
            )
            key = (repository, tag, owner)
            if key not in snapshot.leases:
                return False
            leases = dict(snapshot.leases)
            leases.pop(key, None)
            self._save_unlocked(
                snapshot.records,
                leases,
                generation=snapshot.generation + 1,
            )
            return True

    def prune_expired_leases(self, *, now: datetime | None = None) -> int:
        timestamp = _as_utc(now or datetime.now(timezone.utc))
        with _registry_file_lock(self.path):
            before = self._load_snapshot_unlocked()
            after = self._prune_expired_leases_unlocked(before, now=timestamp)
            return len(before.leases) - len(after.leases)

    @contextmanager
    def lease_fence(
        self,
        *,
        expected_generation: int | None = None,
        now: datetime | None = None,
    ) -> Iterator[RegistryUsageSnapshot]:
        """Hold the usage-file lock across one bounded remote delete decision."""

        timestamp = _as_utc(now or datetime.now(timezone.utc))
        with _registry_file_lock(self.path):
            snapshot = self._prune_expired_leases_unlocked(
                self._load_snapshot_unlocked(),
                now=timestamp,
            )
            if (
                expected_generation is not None
                and snapshot.generation != expected_generation
            ):
                raise RegistryUsageGenerationChanged(
                    "registry usage or active leases changed while pruning was planned"
                )
            yield snapshot

    def _load_snapshot_unlocked(self) -> RegistryUsageSnapshot:
        if not self.path.exists():
            return RegistryUsageSnapshot(generation=0, records={}, leases={})
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("registry usage store must contain a JSON object.")
        items = raw.get("images", [])
        if not isinstance(items, list):
            raise ValueError("registry usage store must contain an images list.")
        records: dict[tuple[str, str], RegistryImageUsage] = {}
        for item in items:
            record = RegistryImageUsage.from_dict(item)
            if record is None:
                continue
            records[(record.repository, record.tag)] = record
        raw_leases = raw.get("leases", [])
        leases: dict[tuple[str, str, str], RegistryImageLease] = {}
        if not isinstance(raw_leases, list):
            raise ValueError("registry usage store must contain a leases list.")
        for index, item in enumerate(raw_leases):
            lease = RegistryImageLease.from_dict(item)
            if lease is None:
                # A malformed/partially-written lease must never disappear from
                # prune protection as though it had expired.
                raise ValueError(
                    f"registry usage store contains an invalid lease at index {index}."
                )
            if lease.key in leases:
                raise ValueError("registry usage store contains a duplicate lease.")
            leases[lease.key] = lease
        if "generation" not in raw:
            generation = 0
        else:
            try:
                generation = int(raw["generation"])
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "registry usage store generation must be an integer."
                ) from exc
            if generation < 0:
                raise ValueError("registry usage store generation cannot be negative.")
        return RegistryUsageSnapshot(
            generation=generation,
            records=records,
            leases=leases,
        )

    def _save_unlocked(
        self,
        records: dict[tuple[str, str], RegistryImageUsage],
        leases: dict[tuple[str, str, str], RegistryImageLease],
        *,
        generation: int,
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_name(
            f"{self.path.name}.tmp-{os.getpid()}-{get_ident()}-{time.monotonic_ns()}"
        )
        payload = {
            "generation": generation,
            "images": [
                records[key].to_dict()
                for key in sorted(records, key=lambda item: (item[0], item[1]))
            ],
            "leases": [
                leases[key].to_dict()
                for key in sorted(
                    leases,
                    key=lambda item: (item[0], item[1], item[2]),
                )
            ],
        }
        try:
            descriptor = os.open(
                tmp_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
            try:
                _adopt_shared_state_owner(
                    descriptor,
                    self.path if self.path.exists() else self.path.parent,
                )
            except BaseException:
                os.close(descriptor)
                raise
            with os.fdopen(descriptor, "w", encoding="utf-8") as file:
                file.write(json.dumps(payload, indent=2, sort_keys=True))
                file.flush()
                os.fsync(file.fileno())
            os.replace(tmp_path, self.path)
            os.chmod(self.path, 0o600)
            directory_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass

    def _prune_expired_leases_unlocked(
        self,
        snapshot: RegistryUsageSnapshot,
        *,
        now: datetime,
    ) -> RegistryUsageSnapshot:
        active = {
            key: lease for key, lease in snapshot.leases.items() if lease.is_active(now)
        }
        if len(active) == len(snapshot.leases):
            return snapshot
        generation = snapshot.generation + 1
        self._save_unlocked(snapshot.records, active, generation=generation)
        return RegistryUsageSnapshot(
            generation=generation,
            records=snapshot.records,
            leases=active,
        )


def registry_prune_plan(
    client: RegistryClient,
    *,
    keep_per_repository: int,
    repository_prefix: str = "",
    max_age_days: float | None = None,
    usage_records: dict[tuple[str, str], RegistryImageUsage] | None = None,
    active_leases: Mapping[tuple[str, str, str], RegistryImageLease] | None = None,
    usage_generation: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    records = list_registry_tags(client, repository_prefix=repository_prefix)
    records = apply_registry_usage(records, usage_records)
    use_last_used_at = usage_records is not None
    candidates = select_prune_candidates(
        records,
        keep_per_repository=keep_per_repository,
        max_age_days=max_age_days,
        use_last_used_at=use_last_used_at,
        active_leases=active_leases,
        now=now,
    )
    return {
        "age_basis": "last_used_at" if use_last_used_at else "created_at",
        "keep_per_repository": keep_per_repository,
        "repository_prefix": repository_prefix,
        "max_age_days": max_age_days,
        "usage_generation": usage_generation,
        "active_lease_count": len(_active_leases(active_leases, now=now)),
        "tags": [record.to_dict() for record in records],
        "delete": [record.to_dict() for record in candidates],
    }


def execute_registry_prune(
    client: RegistryClient,
    records: list[RegistryTag],
    *,
    usage_store: RegistryUsageStore | None = None,
    expected_usage_generation: int | None = None,
    revalidate: Callable[[RegistryTag], bool] | None = None,
    all_records: list[RegistryTag] | None = None,
    now: datetime | None = None,
) -> list[RegistryTag]:
    if usage_store is not None and all_records is None:
        raise ValueError(
            "all_records is required with usage_store so digest aliases are fenced"
        )
    grouped: dict[tuple[str, str], list[RegistryTag]] = {}
    for record in records:
        key = (record.repository, record.digest)
        grouped.setdefault(key, []).append(record)
    all_grouped: dict[tuple[str, str], list[RegistryTag]] = {}
    for record in all_records or records:
        all_grouped.setdefault((record.repository, record.digest), []).append(record)
    deleted: list[RegistryTag] = []
    for (repository, digest), aliases in grouped.items():
        digest_aliases = all_grouped.get((repository, digest), aliases)
        if usage_store is not None:
            # The cross-process store lock remains held through this one remote
            # delete. RegistryClient bounds the critical section with its
            # request timeout; other digest decisions release and reacquire it.
            with usage_store.lease_fence(
                expected_generation=expected_usage_generation,
                now=now,
            ) as snapshot:
                leased_tags = snapshot.active_lease_tags(now=now)
                leased_digests = snapshot.active_lease_digests(now=now)
                if (repository, digest) in leased_digests:
                    continue
                if any(
                    (record.repository, record.tag) in leased_tags
                    for record in digest_aliases
                ):
                    continue
                if revalidate is not None and not all(
                    revalidate(record) for record in digest_aliases
                ):
                    continue
                client.delete_manifest(repository, digest)
        else:
            if revalidate is not None and not all(
                revalidate(record) for record in digest_aliases
            ):
                continue
            client.delete_manifest(repository, digest)
        deleted.extend(aliases)
    return deleted


@contextmanager
def registry_maintenance_lock(
    path: Path,
    *,
    blocking: bool = True,
) -> Iterator[None]:
    """Fence prune/GC processes that share a maintenance lock path."""

    try:
        with _registry_file_lock(path, blocking=blocking):
            yield
    except BlockingIOError as exc:
        raise RegistryMaintenanceBusy(
            f"registry maintenance is already active: {path}"
        ) from exc


def list_registry_tags(
    client: RegistryClient,
    *,
    repository_prefix: str = "",
) -> list[RegistryTag]:
    records: list[RegistryTag] = []
    for repository in client.catalog():
        if repository_prefix and not repository.startswith(repository_prefix):
            continue
        try:
            tags = client.tags(repository)
        except RegistryRequestError as exc:
            if _registry_repository_name_unknown(exc):
                continue
            raise
        for tag in tags:
            record = client.tag_record(repository, tag)
            if record is not None:
                records.append(record)
    return records


def registry_summary(
    client: RegistryClient,
    *,
    max_repositories: int = 24,
    max_tags_per_repository: int = 50,
) -> dict[str, Any]:
    repositories = sorted(client.catalog())
    scanned = repositories[: max(0, max_repositories)]
    records: list[dict[str, Any]] = []
    scanned_tag_count = 0
    visible_tag_count_total = 0
    internal_tag_count_total = 0
    unavailable_records: list[dict[str, Any]] = []
    for repository in scanned:
        try:
            all_tags = sorted(client.tags(repository))
        except RegistryRequestError as exc:
            if not _registry_repository_name_unknown(exc):
                raise
            record = {
                "repository": repository,
                "namespace": repository.split("/", 1)[0] if "/" in repository else "",
                "available": False,
                "error": "repository listed in catalog but tags are unavailable",
                "tag_count": 0,
                "visible_tag_count": 0,
                "tags_truncated": False,
                "latest_tag": "",
                "tags": [],
            }
            records.append(record)
            unavailable_records.append(record)
            continue
        tags = [tag for tag in all_tags if not is_digest_protection_tag(tag)]
        internal_tag_count = len(all_tags) - len(tags)
        visible_tag_limit = max(0, max_tags_per_repository)
        visible_tags = tags[-visible_tag_limit:] if visible_tag_limit else []
        scanned_tag_count += len(tags)
        visible_tag_count_total += len(visible_tags)
        internal_tag_count_total += internal_tag_count
        records.append(
            {
                "repository": repository,
                "namespace": repository.split("/", 1)[0] if "/" in repository else "",
                "available": True,
                "tag_count": len(tags),
                "internal_tag_count": internal_tag_count,
                "visible_tag_count": len(visible_tags),
                "tags_truncated": len(visible_tags) < len(tags),
                "latest_tag": visible_tags[-1] if visible_tags else "",
                "tags": visible_tags,
            }
        )
    return {
        "configured": True,
        "ok": True,
        "url": client.base_url,
        "repository_count": len(repositories),
        "scanned_repository_count": len(scanned),
        "scanned_tag_count": scanned_tag_count,
        "visible_tag_count": visible_tag_count_total,
        "internal_tag_count": internal_tag_count_total,
        "unavailable_repository_count": len(unavailable_records),
        "unavailable_repositories": [
            record["repository"] for record in unavailable_records
        ],
        "catalog_truncated": len(scanned) < len(repositories),
        "repositories": records,
    }


def select_prune_candidates(
    records: list[RegistryTag],
    *,
    keep_per_repository: int,
    max_age_days: float | None = None,
    use_last_used_at: bool = False,
    active_leases: Mapping[tuple[str, str, str], RegistryImageLease] | None = None,
    now: datetime | None = None,
) -> list[RegistryTag]:
    keep = max(0, keep_per_repository)
    cutoff = _age_cutoff(max_age_days, now=now)
    candidates: list[RegistryTag] = []
    by_repository: dict[str, list[RegistryTag]] = {}
    for record in records:
        by_repository.setdefault(record.repository, []).append(record)
    leased_tags = {
        (lease.repository, lease.tag)
        for lease in _active_leases(active_leases, now=now)
        if not lease.digest
    }
    leased_digests = {
        (lease.repository, lease.digest)
        for lease in _active_leases(active_leases, now=now)
        if lease.digest
    }
    for repository_records in by_repository.values():
        ordered = sorted(
            repository_records,
            key=lambda item: _tag_sort_key(item, use_last_used_at=use_last_used_at),
            reverse=True,
        )
        protected_digests: set[str] = set()
        for record in ordered:
            if len(protected_digests) >= keep:
                break
            protected_digests.add(record.digest)
        protected_digests.update(
            record.digest
            for record in ordered
            if (record.repository, record.tag) in leased_tags
        )
        protected_digests.update(
            record.digest
            for record in ordered
            if (record.repository, record.digest) in leased_digests
        )
        for record in ordered:
            if record.digest in protected_digests:
                continue
            if cutoff is not None and not _tag_age_before(
                record,
                cutoff,
                use_last_used_at=use_last_used_at,
            ):
                continue
            candidates.append(record)
    return sorted(candidates, key=lambda item: (item.repository, item.tag))


def apply_registry_usage(
    records: list[RegistryTag],
    usage_records: dict[tuple[str, str], RegistryImageUsage] | None,
) -> list[RegistryTag]:
    if usage_records is None:
        return records
    annotated: list[RegistryTag] = []
    for record in records:
        usage = usage_records.get((record.repository, record.tag))
        annotated.append(
            RegistryTag(
                repository=record.repository,
                tag=record.tag,
                digest=record.digest,
                created_at=record.created_at,
                last_used_at=usage.last_used_at if usage is not None else "",
            )
        )
    return annotated


def _active_leases(
    leases: Mapping[tuple[str, str, str], RegistryImageLease] | None,
    *,
    now: datetime | None,
) -> list[RegistryImageLease]:
    if not leases:
        return []
    reference = _as_utc(now or datetime.now(timezone.utc))
    return [lease for lease in leases.values() if lease.is_active(reference)]


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _validate_lease_identity(
    repository: str,
    tag: str,
    owner: str,
) -> tuple[str, str, str]:
    cleaned = tuple(str(value).strip() for value in (repository, tag, owner))
    labels = ("repository", "tag", "owner")
    for label, value in zip(labels, cleaned):
        if not value:
            raise ValueError(f"registry lease {label} is required")
        if len(value) > 256:
            raise ValueError(f"registry lease {label} is too long")
        if "\n" in value or "\r" in value:
            raise ValueError(f"registry lease {label} cannot contain newlines")
    return cleaned


def _validate_lease_ttl(value: float) -> float:
    try:
        ttl = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("registry lease TTL must be a finite number") from exc
    if not math.isfinite(ttl) or ttl <= 0:
        raise ValueError("registry lease TTL must be a positive finite number")
    if ttl > MAX_REGISTRY_LEASE_TTL_SECONDS:
        raise ValueError(
            f"registry lease TTL cannot exceed {MAX_REGISTRY_LEASE_TTL_SECONDS} seconds"
        )
    return ttl


def _validate_lease_digest(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    digest = normalize_manifest_digest(raw)
    if not digest:
        raise ValueError("registry lease digest must be a valid sha256 digest")
    return digest


def _age_cutoff(
    max_age_days: float | None,
    *,
    now: datetime | None,
) -> datetime | None:
    if max_age_days is None:
        return None
    reference = now or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return reference.astimezone(timezone.utc) - timedelta(days=max_age_days)


def _tag_age_before(
    record: RegistryTag,
    cutoff: datetime,
    *,
    use_last_used_at: bool,
) -> bool:
    raw_timestamp = record.last_used_at if use_last_used_at else record.created_at
    timestamp = parse_iso_datetime(raw_timestamp)
    if timestamp is None:
        return False
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc) < cutoff


def _tag_sort_key(
    record: RegistryTag,
    *,
    use_last_used_at: bool = False,
) -> tuple[int, str, str]:
    raw_timestamp = record.last_used_at if use_last_used_at else record.created_at
    timestamp = parse_iso_datetime(raw_timestamp)
    if timestamp is None and use_last_used_at:
        timestamp = parse_iso_datetime(record.created_at)
    if timestamp is None:
        return (0, "", record.tag)
    return (1, timestamp.isoformat(), record.tag)


def registry_repository_tag_from_image_ref(image_ref: str) -> tuple[str, str] | None:
    image = image_ref.strip()
    if not image or "://" in image:
        return None
    image = image.split("@", 1)[0]
    if not image:
        return None
    components = image.split("/")
    if len(components) > 1 and (
        "." in components[0] or ":" in components[0] or components[0] == "localhost"
    ):
        components = components[1:]
    if not components:
        return None
    last = components[-1]
    if ":" in last:
        name, tag = last.rsplit(":", 1)
        if not name or not tag:
            return None
        components[-1] = name
    else:
        tag = "latest"
    repository = "/".join(part for part in components if part)
    if not repository:
        return None
    return repository, tag


def manifest_digest_from_image_ref(image_ref: str) -> str:
    """Return a normalized digest from a pinned image reference, if present."""

    _separator, found, raw_digest = image_ref.strip().rpartition("@")
    if not found:
        return ""
    return normalize_manifest_digest(raw_digest)


def normalize_manifest_digest(digest: str) -> str:
    normalized = digest.strip().lower()
    return normalized if _MANIFEST_DIGEST_RE.fullmatch(normalized) else ""


def digest_protection_tag(digest: str) -> str:
    normalized = _validate_lease_digest(digest)
    algorithm, hexadecimal = normalized.split(":", 1)
    return f"ucloud-digest-{algorithm}-{hexadecimal}"


def is_digest_protection_tag(tag: str) -> bool:
    return bool(_DIGEST_PROTECTION_TAG_RE.fullmatch(tag.strip().lower()))


def image_ref_with_manifest_digest(image_ref: str, digest: str) -> str:
    """Pin ``image_ref`` while retaining its optional human-readable tag."""

    normalized_digest = normalize_manifest_digest(digest)
    image = image_ref.strip().split("@", 1)[0]
    if not image or not normalized_digest:
        return image_ref.strip()
    return f"{image}@{normalized_digest}"


def canonical_image_digest_ref(image_ref: str, digest: str = "") -> str:
    """Return the repository@digest identity used for cache comparisons."""

    normalized_digest = normalize_manifest_digest(
        digest or manifest_digest_from_image_ref(image_ref)
    )
    image = image_ref.strip().split("@", 1)[0]
    if not image or not normalized_digest:
        return ""
    prefix, separator, last = image.rpartition("/")
    if ":" in last:
        last = last.rsplit(":", 1)[0]
    if not last:
        return ""
    repository = f"{prefix}{separator}{last}" if prefix else last
    return f"{repository}@{normalized_digest}"


def registry_host_from_image_ref(image_ref: str) -> str:
    image = image_ref.strip()
    if not image or "://" in image:
        return ""
    if "/" not in image:
        return ""
    first = image.split("/", 1)[0]
    if "." in first or ":" in first or first == "localhost":
        return first
    return ""


def _registry_repository_name_unknown(exc: RegistryRequestError) -> bool:
    if exc.status_code != 404:
        return False
    try:
        payload = json.loads(exc.body)
    except json.JSONDecodeError:
        return False
    errors = payload.get("errors") if isinstance(payload, dict) else None
    if not isinstance(errors, list):
        return False
    return any(
        isinstance(item, dict) and item.get("code") == "NAME_UNKNOWN" for item in errors
    )


def _quote_repository(repository: str) -> str:
    return quote(repository.strip("/"), safe="/")


def _next_link_path(link: str | None) -> str:
    if not link:
        return ""
    for part in link.split(","):
        if 'rel="next"' not in part and "rel=next" not in part:
            continue
        start = part.find("<")
        end = part.find(">", start + 1)
        if start < 0 or end <= start:
            continue
        target = part[start + 1 : end]
        parsed = urlparse(target)
        path = parsed.path or target
        if parsed.query:
            path = f"{path}?{parsed.query}"
        return path
    return ""


@contextmanager
def _registry_file_lock(
    path: Path,
    *,
    blocking: bool = True,
) -> Iterator[None]:
    resolved = Path(path).resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with _REGISTRY_FILE_LOCKS_GUARD:
        local_lock = _REGISTRY_FILE_LOCKS.get(resolved)
        if local_lock is None:
            local_lock = RLock()
            _REGISTRY_FILE_LOCKS[resolved] = local_lock
    acquired = local_lock.acquire(blocking=blocking)
    if not acquired:
        raise BlockingIOError(f"lock is already held: {resolved}")
    try:
        lock_path = resolved.with_name(resolved.name + ".lock")
        with lock_path.open("a+", encoding="utf-8") as lock_file:
            _adopt_shared_state_owner(lock_file.fileno(), resolved.parent)
            flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
            fcntl.flock(lock_file.fileno(), flags)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    finally:
        local_lock.release()


def _adopt_shared_state_owner(descriptor: int, owner_source: Path) -> None:
    """Keep root maintenance writes accessible to the service account.

    Atomic replacement creates a new inode owned by the writing process. The
    registry maintenance jobs run as root while the gateway runs as the owner
    of the state directory, so root must explicitly retain that shared owner.
    """

    if os.geteuid() != 0:
        return
    try:
        ownership = owner_source.stat()
    except FileNotFoundError:
        ownership = owner_source.parent.stat()
    os.fchown(descriptor, ownership.st_uid, ownership.st_gid)
