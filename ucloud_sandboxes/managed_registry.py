from __future__ import annotations

from contextlib import contextmanager
from dataclasses import astuple, dataclass, field
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
from threading import RLock
from typing import Any, Callable, Iterable, Iterator, Mapping
from urllib import error, request
from urllib.parse import quote, unquote, urlencode, urljoin, urlparse, urlunparse

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
_REGISTRY_USAGE_ERROR = "registry usage database is invalid or unavailable"


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
class RegistryLayerDescriptor:
    digest: str
    size: int


@dataclass(frozen=True)
class RegistryManifestLayers:
    repository: str
    manifest_digest: str
    layers: tuple[RegistryLayerDescriptor, ...]

    @property
    def total_size(self) -> int:
        return sum(layer.size for layer in self.layers)


@dataclass(frozen=True)
class RegistryImageUsage:
    image_ref: str
    repository: str
    tag: str
    last_used_at: str


@dataclass(frozen=True)
class RegistryImageLease:
    repository: str
    tag: str
    owner: str
    acquired_at: str
    renewed_at: str
    expires_at: str
    digest: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.repository, self.tag, self.owner)

    def is_active(self, now: datetime) -> bool:
        if not self.expires_at:
            return True
        expires_at = parse_iso_datetime(self.expires_at)
        return expires_at is not None and expires_at > _as_utc(now)


@dataclass(frozen=True)
class RegistryUsageSnapshot:
    generation: int
    records: dict[tuple[str, str], RegistryImageUsage]
    leases: dict[tuple[str, str, str], RegistryImageLease] = field(default_factory=dict)

    def active_lease_digests(
        self,
        *,
        now: datetime | None = None,
    ) -> set[tuple[str, str]]:
        reference = _as_utc(now or datetime.now(timezone.utc))
        return {
            (lease.repository, lease.digest)
            for lease in self.leases.values()
            if lease.is_active(reference)
        }


class RegistryUsageGenerationChanged(RuntimeError):
    pass


class RegistryUsageStateError(ValueError):
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


class _CaseInsensitiveHeaders(dict[str, str]):
    def __init__(self, items: Iterable[tuple[str, str]]) -> None:
        values: dict[str, str] = {}
        self._names: dict[str, str] = {}
        for name, value in items:
            folded = name.casefold()
            previous = self._names.get(folded)
            if previous is not None:
                values.pop(previous, None)
            self._names[folded] = name
            values[name] = value
        super().__init__(values)

    def __getitem__(self, key: str) -> str:
        return super().__getitem__(self._names.get(key.casefold(), key))

    def __contains__(self, key: object) -> bool:
        if isinstance(key, str):
            return key.casefold() in self._names
        return False

    def get(self, key: str, default: Any = None) -> Any:
        return super().get(self._names.get(key.casefold(), key), default)


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
            path = _next_link_path(
                headers.get("Link"),
                current_path=path,
                base_url=self.base_url,
            )
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
            path = _next_link_path(
                headers.get("Link"),
                current_path=path,
                base_url=self.base_url,
            )
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

    def manifest_layers(
        self,
        repository: str,
        reference: str,
    ) -> RegistryManifestLayers:
        """Return compressed layer digests and sizes for one Linux/amd64 image."""

        return self._manifest_layers(repository, reference, depth=0)

    def manifest_document(
        self,
        repository: str,
        reference: str,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        return self._json_request(
            (
                f"/v2/{_quote_repository(repository)}/manifests/"
                f"{quote(reference, safe=':')}"
            ),
            headers={"Accept": MANIFEST_ACCEPT},
        )

    def blob_bytes(
        self,
        repository: str,
        digest: str,
        *,
        max_bytes: int = 16 * 1024 * 1024,
    ) -> bytes:
        normalized = _validate_lease_digest(digest)
        if max_bytes <= 0:
            raise ValueError("registry blob byte limit must be positive")
        response = self._request(
            (
                f"/v2/{_quote_repository(repository)}/blobs/"
                f"{quote(normalized, safe=':')}"
            )
        )
        try:
            payload = response.read(max_bytes + 1)
        finally:
            response.close()
        if len(payload) > max_bytes:
            raise ValueError("registry blob exceeds the configured byte limit")
        return payload

    def _manifest_layers(
        self,
        repository: str,
        reference: str,
        *,
        depth: int,
    ) -> RegistryManifestLayers:
        if depth > 1:
            raise ValueError("registry manifest index nesting is too deep")
        path = (
            f"/v2/{_quote_repository(repository)}/manifests/"
            f"{quote(reference, safe=':')}"
        )
        manifest, headers = self._json_request(
            path,
            headers={"Accept": MANIFEST_ACCEPT},
        )
        raw_layers = manifest.get("layers")
        if isinstance(raw_layers, list):
            layers: list[RegistryLayerDescriptor] = []
            for raw in raw_layers:
                if not isinstance(raw, dict):
                    raise ValueError("registry manifest contains an invalid layer")
                digest = normalize_manifest_digest(str(raw.get("digest") or ""))
                size = raw.get("size")
                if not isinstance(size, int) or isinstance(size, bool):
                    raise ValueError(
                        "registry manifest layer size must be an integer"
                    )
                if not digest or size < 0:
                    raise ValueError("registry manifest contains an invalid layer")
                layers.append(RegistryLayerDescriptor(digest=digest, size=size))
            manifest_digest = normalize_manifest_digest(
                str(headers.get("Docker-Content-Digest") or "")
            ) or normalize_manifest_digest(reference)
            if not manifest_digest:
                raise ValueError("registry manifest response is missing its digest")
            return RegistryManifestLayers(
                repository=repository,
                manifest_digest=manifest_digest,
                layers=tuple(layers),
            )

        raw_manifests = manifest.get("manifests")
        if not isinstance(raw_manifests, list) or not raw_manifests:
            raise ValueError("registry response is neither an image manifest nor index")
        selected = _select_linux_amd64_manifest(raw_manifests)
        digest = normalize_manifest_digest(str(selected.get("digest") or ""))
        if not digest:
            raise ValueError("registry manifest index entry is missing its digest")
        return self._manifest_layers(repository, digest, depth=depth + 1)

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

    def blob_exists(self, repository: str, digest: str) -> bool:
        normalized = _validate_lease_digest(digest)
        path = (
            f"/v2/{_quote_repository(repository)}/blobs/"
            f"{quote(normalized, safe=':')}"
        )
        try:
            response = self._request(path, method="HEAD")
        except RegistryRequestError as exc:
            if exc.status_code == 404:
                return False
            raise
        response.close()
        return True

    def start_blob_upload(self, repository: str) -> str:
        response = self._request(
            f"/v2/{_quote_repository(repository)}/blobs/uploads/",
            method="POST",
            data=b"",
        )
        try:
            return self._upload_location_path(response)
        finally:
            response.close()

    def upload_blob_chunk(self, location: str, chunk: bytes) -> str:
        if not chunk:
            raise ValueError("registry upload chunk cannot be empty")
        response = self._request(
            self._validate_upload_location(location),
            method="PATCH",
            headers={"Content-Type": "application/octet-stream"},
            data=chunk,
        )
        try:
            return self._upload_location_path(response)
        finally:
            response.close()

    def finish_blob_upload(self, location: str, digest: str) -> str:
        normalized = _validate_lease_digest(digest)
        path = self._validate_upload_location(location)
        separator = "&" if "?" in path else "?"
        response = self._request(
            f"{path}{separator}{urlencode({'digest': normalized})}",
            method="PUT",
            headers={"Content-Type": "application/octet-stream"},
            data=b"",
        )
        try:
            stored = normalize_manifest_digest(
                str(response.headers.get("Docker-Content-Digest") or "")
            )
        finally:
            response.close()
        if stored and stored != normalized:
            raise ValueError("registry stored blob under an unexpected digest")
        return normalized

    def put_manifest(
        self,
        repository: str,
        reference: str,
        payload: bytes,
        *,
        media_type: str = "application/vnd.oci.image.manifest.v1+json",
    ) -> str:
        if not payload:
            raise ValueError("registry manifest cannot be empty")
        response = self._request(
            (
                f"/v2/{_quote_repository(repository)}/manifests/"
                f"{quote(reference, safe=':')}"
            ),
            method="PUT",
            headers={"Content-Type": media_type},
            data=payload,
        )
        try:
            digest = normalize_manifest_digest(
                str(response.headers.get("Docker-Content-Digest") or "")
            )
        finally:
            response.close()
        return digest or f"sha256:{hashlib.sha256(payload).hexdigest()}"

    def _upload_location_path(self, response: Any) -> str:
        location = str(response.headers.get("Location") or "")
        if not location:
            raise ValueError("registry upload response is missing Location")
        return self._validate_upload_location(location)

    def _validate_upload_location(self, location: str) -> str:
        parsed = urlparse(location)
        if parsed.fragment:
            raise ValueError("registry upload Location must not contain a fragment")
        if parsed.scheme or parsed.netloc:
            base = urlparse(self.base_url)
            if not _same_url_origin(parsed, base):
                raise ValueError("registry upload redirected to another origin")
        path_component = parsed.path
        if parsed.params:
            path_component += f";{parsed.params}"
        path = urlunparse(("", "", parsed.path, parsed.params, parsed.query, ""))
        if not _is_safe_registry_v2_path(path_component):
            raise ValueError("registry upload Location is outside /v2/")
        return path

    def _json_request(
        self,
        path: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any], Any]:
        response = self._request(path, headers=headers)
        try:
            body = response.read(MAX_REGISTRY_JSON_RESPONSE_BYTES + 1)
            response_headers = _CaseInsensitiveHeaders(response.headers.items())
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
    _COLUMNS = {
        "registry_meta": "singleton generation",
        "registry_images": "image_ref repository tag last_used_at",
        "registry_leases": "repository tag owner acquired_at renewed_at expires_at digest",
    }
    _SCHEMA = """
        BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS registry_meta (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), generation INTEGER NOT NULL CHECK (generation >= 0)) STRICT;
        CREATE TABLE IF NOT EXISTS registry_images (image_ref TEXT NOT NULL, repository TEXT NOT NULL, tag TEXT NOT NULL, last_used_at TEXT NOT NULL, PRIMARY KEY (repository, tag)) STRICT;
        CREATE TABLE IF NOT EXISTS registry_leases (repository TEXT NOT NULL, tag TEXT NOT NULL, owner TEXT NOT NULL, acquired_at TEXT NOT NULL, renewed_at TEXT NOT NULL, expires_at TEXT NOT NULL, digest TEXT NOT NULL, PRIMARY KEY (repository, tag, owner)) STRICT;
        INSERT OR IGNORE INTO registry_meta VALUES (1, 0);
        PRAGMA user_version = 1;
        COMMIT;
    """
    _USAGE_UPSERT = "INSERT OR REPLACE INTO registry_images VALUES (?, ?, ?, ?)"

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            # Version and schema must come from one snapshot. Without this
            # lock, two first-openers can observe user_version=0 before the
            # other process commits, then observe its newly committed tables
            # and misclassify a valid database as an unversioned legacy file.
            conn.execute("BEGIN IMMEDIATE")
            version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if version == 0 and tables:
                raise sqlite3.DatabaseError(
                    "unsupported registry usage schema version 0"
                )
            if version == 0:
                conn.execute(
                    "CREATE TABLE registry_meta "
                    "(singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
                    "generation INTEGER NOT NULL CHECK (generation >= 0)) STRICT"
                )
                conn.execute(
                    "CREATE TABLE registry_images "
                    "(image_ref TEXT NOT NULL, repository TEXT NOT NULL, "
                    "tag TEXT NOT NULL, last_used_at TEXT NOT NULL, "
                    "PRIMARY KEY (repository, tag)) STRICT"
                )
                conn.execute(
                    "CREATE TABLE registry_leases "
                    "(repository TEXT NOT NULL, tag TEXT NOT NULL, "
                    "owner TEXT NOT NULL, acquired_at TEXT NOT NULL, "
                    "renewed_at TEXT NOT NULL, expires_at TEXT NOT NULL, "
                    "digest TEXT NOT NULL, "
                    "PRIMARY KEY (repository, tag, owner)) STRICT"
                )
                conn.execute("INSERT INTO registry_meta VALUES (1, 0)")
                conn.execute("PRAGMA user_version = 1")
                tables = set(self._COLUMNS)
            elif version != 1:
                raise sqlite3.DatabaseError(
                    f"unsupported registry usage schema version {version}"
                )
            if tables != set(self._COLUMNS):
                raise sqlite3.DatabaseError("invalid registry usage database schema")
            for table, expected in self._COLUMNS.items():
                columns = " ".join(
                    row[1] for row in conn.execute(f"PRAGMA table_info({table})")
                )
                strict = conn.execute(
                    "SELECT strict FROM pragma_table_list WHERE name = ?", (table,)
                ).fetchone()
                if columns != expected or strict is None or strict[0] != 1:
                    raise sqlite3.DatabaseError(
                        f"invalid registry usage table: {table}"
                    )
            journal = conn.execute("PRAGMA journal_mode").fetchone()
            if journal is None or str(journal[0]).lower() != "delete":
                raise sqlite3.DatabaseError(
                    "registry usage database requires DELETE journal mode"
                )
            conn.commit()
        except sqlite3.Error as exc:
            if conn.in_transaction:
                conn.rollback()
            raise RegistryUsageStateError(_REGISTRY_USAGE_ERROR) from exc
        finally:
            conn.close()
        os.chmod(self.path, 0o600)
        with self.path.open("rb") as database:
            _adopt_shared_state_owner(database.fileno(), self.path.parent)

    def _connect(self) -> sqlite3.Connection:
        try:
            return sqlite3.connect(self.path, timeout=60, isolation_level=None)
        except sqlite3.Error as exc:
            raise RegistryUsageStateError(_REGISTRY_USAGE_ERROR) from exc

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException as exc:
            if conn.in_transaction:
                conn.rollback()
            if isinstance(exc, sqlite3.Error):
                raise RegistryUsageStateError(_REGISTRY_USAGE_ERROR) from exc
            raise
        finally:
            conn.close()

    def load(self) -> dict[tuple[str, str], RegistryImageUsage]:
        return self.snapshot().records

    def snapshot(self, *, now: datetime | None = None) -> RegistryUsageSnapshot:
        with self._transaction() as conn:
            return self._prune_expired_unlocked(
                conn,
                now=_as_utc(now or datetime.now(timezone.utc)),
            )

    def save(
        self,
        records: dict[tuple[str, str], RegistryImageUsage],
        *,
        expected_generation: int | None = None,
    ) -> int:
        with self._transaction() as conn:
            snapshot = self._prune_expired_unlocked(
                conn,
                now=datetime.now(timezone.utc),
            )
            if (
                expected_generation is not None
                and snapshot.generation != expected_generation
            ):
                raise RegistryUsageGenerationChanged(
                    "registry usage changed while maintenance was planned"
                )
            conn.execute("DELETE FROM registry_images")
            conn.executemany(self._USAGE_UPSERT, map(astuple, records.values()))
            return self._advance_generation_unlocked(conn)

    def touch_image(
        self,
        image_ref: str,
        *,
        when: datetime | None = None,
    ) -> RegistryImageUsage | None:
        return next(iter(self.touch_images((image_ref,), when=when)), None)

    def touch_images(
        self,
        image_refs: Iterable[str],
        *,
        when: datetime | None = None,
    ) -> tuple[RegistryImageUsage, ...]:
        timestamp = _as_utc(when or datetime.now(timezone.utc))
        records = [
            RegistryImageUsage(image_ref, parsed[0], parsed[1], timestamp.isoformat())
            for image_ref in image_refs
            if (parsed := registry_repository_tag_from_image_ref(image_ref)) is not None
        ]
        if not records:
            return ()
        with self._transaction() as conn:
            self._prune_expired_unlocked(conn, now=timestamp)
            conn.executemany(self._USAGE_UPSERT, map(astuple, records))
            self._advance_generation_unlocked(conn)
        return tuple(records)

    def acquire_lease(
        self,
        repository: str,
        tag: str,
        owner: str,
        *,
        ttl_seconds: float,
        digest: str,
        now: datetime | None = None,
    ) -> RegistryImageLease:
        return self._put_lease(repository, tag, owner, ttl_seconds, digest, now)

    def acquire_reference(
        self,
        repository: str,
        tag: str,
        owner: str,
        *,
        digest: str,
        now: datetime | None = None,
    ) -> RegistryImageLease:
        repository, tag, owner = _validate_lease_identity(repository, tag, owner)
        digest = _validate_lease_digest(digest)
        timestamp = _as_utc(now or datetime.now(timezone.utc))
        key = (repository, tag, owner)
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT repository, tag, owner, acquired_at, renewed_at, "
                "expires_at, digest FROM registry_leases "
                "WHERE repository = ? AND tag = ? AND owner = ?",
                key,
            ).fetchone()
            previous = self._lease_from_row(row) if row is not None else None
            if previous is not None and not previous.is_active(timestamp):
                conn.execute(
                    "DELETE FROM registry_leases "
                    "WHERE repository = ? AND tag = ? AND owner = ?",
                    key,
                )
                previous = None
            if previous is not None and previous.digest != digest:
                raise ValueError("registry lease/reference digest is immutable")
            if previous is not None and not previous.expires_at:
                # Permanent references are identity fences, not heartbeats.
                # Re-observing one must be an O(1), write-free operation.
                return previous
            reference = RegistryImageLease(
                repository,
                tag,
                owner,
                previous.acquired_at if previous else timestamp.isoformat(),
                timestamp.isoformat(),
                "",
                digest,
            )
            conn.execute(
                "INSERT OR REPLACE INTO registry_leases VALUES (?, ?, ?, ?, ?, ?, ?)",
                astuple(reference),
            )
            self._advance_generation_unlocked(conn)
            return reference

    def renew_lease(
        self,
        repository: str,
        tag: str,
        owner: str,
        *,
        ttl_seconds: float,
        digest: str,
        now: datetime | None = None,
    ) -> RegistryImageLease:
        return self._put_lease(repository, tag, owner, ttl_seconds, digest, now, True)

    def _put_lease(
        self,
        repository: str,
        tag: str,
        owner: str,
        ttl_seconds: float | None,
        digest: str,
        now: datetime | None,
        require_existing: bool = False,
    ) -> RegistryImageLease:
        repository, tag, owner = _validate_lease_identity(repository, tag, owner)
        digest = _validate_lease_digest(digest)
        timestamp = _as_utc(now or datetime.now(timezone.utc))
        expires_at = (
            ""
            if ttl_seconds is None
            else (
                timestamp + timedelta(seconds=_validate_lease_ttl(ttl_seconds))
            ).isoformat()
        )
        key = (repository, tag, owner)
        lease = None
        with self._transaction() as conn:
            previous = self._prune_expired_unlocked(
                conn,
                now=timestamp,
            ).leases.get(key)
            if previous is not None and previous.digest != digest:
                raise ValueError("registry lease/reference digest is immutable")
            if previous is not None or not require_existing:
                lease = RegistryImageLease(
                    repository,
                    tag,
                    owner,
                    previous.acquired_at if previous else timestamp.isoformat(),
                    timestamp.isoformat(),
                    expires_at,
                    digest,
                )
                conn.execute(
                    "INSERT OR REPLACE INTO registry_leases VALUES (?, ?, ?, ?, ?, ?, ?)",
                    astuple(lease),
                )
                self._advance_generation_unlocked(conn)
        if lease is None:
            raise RegistryImageLeaseNotFound(key)
        return lease

    def release_lease(
        self,
        repository: str,
        tag: str,
        owner: str,
        *,
        now: datetime | None = None,
    ) -> bool:
        key = _validate_lease_identity(repository, tag, owner)
        timestamp = _as_utc(now or datetime.now(timezone.utc))
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT repository, tag, owner, acquired_at, renewed_at, "
                "expires_at, digest FROM registry_leases "
                "WHERE repository = ? AND tag = ? AND owner = ?",
                key,
            ).fetchone()
            if row is None:
                return False
            lease = self._lease_from_row(row)
            conn.execute(
                "DELETE FROM registry_leases "
                "WHERE repository = ? AND tag = ? AND owner = ?",
                key,
            )
            self._advance_generation_unlocked(conn)
            return lease.is_active(timestamp)

    @contextmanager
    def lease_fence(
        self,
        *,
        expected_generation: int | None = None,
        now: datetime | None = None,
    ) -> Iterator[RegistryUsageSnapshot]:
        with self._transaction() as conn:
            snapshot = self._prune_expired_unlocked(
                conn,
                now=_as_utc(now or datetime.now(timezone.utc)),
            )
            if (
                expected_generation is not None
                and snapshot.generation != expected_generation
            ):
                raise RegistryUsageGenerationChanged(
                    "registry usage or active leases changed while pruning was planned"
                )
            yield snapshot

    @staticmethod
    def _generation_unlocked(conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT generation FROM registry_meta").fetchone()
        if row is None or type(row[0]) is not int or row[0] < 0:
            raise ValueError("registry usage generation must be a nonnegative integer")
        return int(row[0])

    @classmethod
    def _advance_generation_unlocked(cls, conn: sqlite3.Connection) -> int:
        conn.execute(
            "UPDATE registry_meta SET generation = generation + 1 "
            "WHERE singleton = 1"
        )
        return cls._generation_unlocked(conn)

    def _snapshot_unlocked(
        self,
        conn: sqlite3.Connection,
    ) -> RegistryUsageSnapshot:
        records = {}
        for row in conn.execute(
            "SELECT image_ref, repository, tag, last_used_at FROM registry_images"
        ):
            if (
                any(type(value) is not str or not value for value in row)
                or parse_iso_datetime(row[3]) is None
            ):
                raise ValueError("registry usage database contains an invalid image")
            record = RegistryImageUsage(*row)
            records[(record.repository, record.tag)] = record
        leases = {}
        for row in conn.execute(
            "SELECT repository, tag, owner, acquired_at, renewed_at, "
            "expires_at, digest FROM registry_leases"
        ):
            lease = self._lease_from_row(row)
            leases[lease.key] = lease
        return RegistryUsageSnapshot(self._generation_unlocked(conn), records, leases)

    @staticmethod
    def _lease_from_row(row: object) -> RegistryImageLease:
        if (
            not isinstance(row, (tuple, sqlite3.Row))
            or len(row) != 7
            or any(type(value) is not str for value in row)
            or any(not value for value in row[:5])
            or parse_iso_datetime(row[3]) is None
            or parse_iso_datetime(row[4]) is None
            or (row[5] and parse_iso_datetime(row[5]) is None)
            or not row[6]
            or normalize_manifest_digest(row[6]) != row[6]
        ):
            raise ValueError("registry usage database contains an invalid lease")
        return RegistryImageLease(*row)

    def _prune_expired_unlocked(
        self,
        conn: sqlite3.Connection,
        *,
        now: datetime,
    ) -> RegistryUsageSnapshot:
        snapshot = self._snapshot_unlocked(conn)
        expired = [
            key for key, lease in snapshot.leases.items() if not lease.is_active(now)
        ]
        if not expired:
            return snapshot
        conn.executemany(
            "DELETE FROM registry_leases "
            "WHERE repository = ? AND tag = ? AND owner = ?",
            expired,
        )
        active = dict(snapshot.leases)
        for key in expired:
            active.pop(key)
        return RegistryUsageSnapshot(
            self._advance_generation_unlocked(conn),
            snapshot.records,
            active,
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
    all_records: list[RegistryTag],
    now: datetime | None = None,
) -> list[RegistryTag]:
    grouped: dict[tuple[str, str], list[RegistryTag]] = {}
    for record in records:
        key = (record.repository, record.digest)
        grouped.setdefault(key, []).append(record)
    all_grouped: dict[tuple[str, str], list[RegistryTag]] = {}
    for record in all_records:
        all_grouped.setdefault((record.repository, record.digest), []).append(record)
    deleted: list[RegistryTag] = []
    for (repository, digest), aliases in grouped.items():
        digest_aliases = all_grouped.get((repository, digest))
        if digest_aliases is None:
            # A candidate absent from the supplied complete inventory is stale
            # or the inventory is incomplete. Either way, deleting is unsafe.
            continue
        selected_aliases = {(record.repository, record.tag) for record in aliases}
        known_aliases = {
            (record.repository, record.tag) for record in digest_aliases
        }
        if selected_aliases != known_aliases:
            # Deleting one manifest digest invalidates every tag alias. When a
            # complete inventory is available, fail closed unless planning
            # independently selected every alias of this digest.
            continue
        if usage_store is not None:
            # The write transaction remains held through this bounded remote
            # delete, serializing new leases and references with the decision.
            with usage_store.lease_fence(
                expected_generation=expected_usage_generation,
                now=now,
            ) as snapshot:
                leased_digests = snapshot.active_lease_digests(now=now)
                if (repository, digest) in leased_digests:
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
    leased_digests = {
        (lease.repository, lease.digest)
        for lease in _active_leases(active_leases, now=now)
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
            if (record.repository, record.digest) in leased_digests
        )
        aliases_by_digest: dict[str, list[RegistryTag]] = {}
        for record in ordered:
            aliases_by_digest.setdefault(record.digest, []).append(record)
        for digest, aliases in aliases_by_digest.items():
            if digest in protected_digests:
                continue
            if cutoff is not None and not all(
                _tag_age_before(
                    alias,
                    cutoff,
                    use_last_used_at=use_last_used_at,
                )
                for alias in aliases
            ):
                continue
            candidates.extend(aliases)
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
    values = (repository, tag, owner)
    if any(not isinstance(value, str) for value in values):
        raise ValueError("registry lease identity fields must be strings")
    cleaned = tuple(value.strip() for value in values)
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
    if not isinstance(value, str):
        raise ValueError("registry lease digest must be a string")
    raw = value.strip()
    if not raw:
        raise ValueError("registry lease digest is required")
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


def _select_linux_amd64_manifest(
    manifests: list[object],
) -> dict[str, Any]:
    valid = [item for item in manifests if isinstance(item, dict)]
    for item in valid:
        platform = item.get("platform")
        if not isinstance(platform, dict):
            continue
        if (
            str(platform.get("os") or "").lower() == "linux"
            and str(platform.get("architecture") or "").lower() == "amd64"
        ):
            return item
    raise ValueError("registry manifest index has no Linux/amd64 image")


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


def _next_link_path(
    link: str | None,
    *,
    current_path: str = "",
    base_url: str = "",
) -> str:
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
        if current_path and base_url:
            base = urlparse(base_url)
            resolved = urlparse(urljoin(f"{base_url}{current_path}", target))
            if resolved.fragment:
                raise ValueError(
                    "registry pagination Link must not contain a fragment"
                )
            if not _same_url_origin(resolved, base):
                raise ValueError("registry pagination Link points to another origin")
            base_path = base.path.rstrip("/")
            if base_path:
                if not (
                    resolved.path == base_path
                    or resolved.path.startswith(f"{base_path}/")
                ):
                    raise ValueError(
                        "registry pagination Link is outside the registry base path"
                    )
                request_path = resolved.path[len(base_path) :] or "/"
            else:
                request_path = resolved.path
            path_component = request_path
            if resolved.params:
                path_component += f";{resolved.params}"
            if not _is_safe_registry_v2_path(path_component):
                raise ValueError("registry pagination Link is outside /v2/")
            return urlunparse(
                ("", "", request_path, resolved.params, resolved.query, "")
            )
        parsed = urlparse(target)
        return urlunparse(("", "", parsed.path, parsed.params, parsed.query, ""))
    return ""


def _same_url_origin(left: Any, right: Any) -> bool:
    try:
        left_port = left.port
        right_port = right.port
    except ValueError:
        return False
    left_scheme = str(left.scheme or "").lower()
    right_scheme = str(right.scheme or "").lower()
    default_ports = {"http": 80, "https": 443}
    return (
        bool(left_scheme)
        and left_scheme == right_scheme
        and str(left.hostname or "").lower() == str(right.hostname or "").lower()
        and (
            left_port if left_port is not None else default_ports.get(left_scheme)
        )
        == (
            right_port if right_port is not None else default_ports.get(right_scheme)
        )
    )


def _is_safe_registry_v2_path(path: str) -> bool:
    decoded_path = path
    for _iteration in range(8):
        unquoted = unquote(decoded_path)
        if unquoted == decoded_path:
            break
        decoded_path = unquoted
    else:
        return False
    return (
        path.startswith("/v2/")
        and "\\" not in decoded_path
        and not any(part in {".", ".."} for part in decoded_path.split("/"))
    )


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
    """Keep root-created shared state accessible to the service account."""

    if os.geteuid() != 0:
        return
    try:
        ownership = owner_source.stat()
    except FileNotFoundError:
        ownership = owner_source.parent.stat()
    os.fchown(descriptor, ownership.st_uid, ownership.st_gid)
