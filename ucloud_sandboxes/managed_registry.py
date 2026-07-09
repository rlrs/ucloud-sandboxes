from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from threading import RLock
from typing import Any
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
        while path:
            payload, headers = self._json_request(path)
            repositories = payload.get("repositories")
            if isinstance(repositories, list):
                found.extend(item for item in repositories if isinstance(item, str))
            path = _next_link_path(headers.get("Link"))
        return found

    def tags(self, repository: str) -> list[str]:
        payload, _headers = self._json_request(f"/v2/{_quote_repository(repository)}/tags/list")
        tags = payload.get("tags")
        return [item for item in tags if isinstance(item, str)] if isinstance(tags, list) else []

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
        response = self._request(path, method="HEAD", headers={"Accept": MANIFEST_ACCEPT})
        digest = response.headers.get("Docker-Content-Digest")
        if digest:
            return digest
        _body, headers = self._json_request(path, headers={"Accept": MANIFEST_ACCEPT})
        return headers.get("Docker-Content-Digest", "")

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
        self._request(
            f"/v2/{_quote_repository(repository)}/manifests/{quote(digest, safe=':')}",
            method="DELETE",
        )

    def _json_request(
        self,
        path: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any], Any]:
        response = self._request(path, headers=headers)
        body = response.read()
        payload = json.loads(body.decode("utf-8")) if body else {}
        if not isinstance(payload, dict):
            raise ValueError(f"registry returned non-object JSON for {path}")
        return payload, response.headers

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
    ) -> Any:
        req = request.Request(
            self.base_url + path,
            method=method,
            headers=headers or {},
        )
        try:
            return request.urlopen(req, timeout=self.timeout_seconds)
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RegistryRequestError(exc.code, method, path, body) from exc


class RegistryUsageStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = RLock()

    def load(self) -> dict[tuple[str, str], RegistryImageUsage]:
        with self._lock:
            if not self.path.exists():
                return {}
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
            return records

    def save(self, records: dict[tuple[str, str], RegistryImageUsage]) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
            payload = {
                "images": [
                    records[key].to_dict()
                    for key in sorted(records, key=lambda item: (item[0], item[1]))
                ]
            }
            tmp_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp_path.replace(self.path)

    def touch_image(
        self,
        image_ref: str,
        *,
        when: datetime | None = None,
    ) -> RegistryImageUsage | None:
        parsed = registry_repository_tag_from_image_ref(image_ref)
        if parsed is None:
            return None
        repository, tag = parsed
        timestamp = when or datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        record = RegistryImageUsage(
            image_ref=image_ref,
            repository=repository,
            tag=tag,
            last_used_at=timestamp.astimezone(timezone.utc).isoformat(),
        )
        with self._lock:
            records = self.load()
            records[(repository, tag)] = record
            self.save(records)
        return record


def registry_prune_plan(
    client: RegistryClient,
    *,
    keep_per_repository: int,
    repository_prefix: str = "",
    max_age_days: float | None = None,
    usage_records: dict[tuple[str, str], RegistryImageUsage] | None = None,
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
        now=now,
    )
    return {
        "age_basis": "last_used_at" if use_last_used_at else "created_at",
        "keep_per_repository": keep_per_repository,
        "repository_prefix": repository_prefix,
        "max_age_days": max_age_days,
        "tags": [record.to_dict() for record in records],
        "delete": [record.to_dict() for record in candidates],
    }


def execute_registry_prune(
    client: RegistryClient,
    records: list[RegistryTag],
) -> list[RegistryTag]:
    deleted: set[tuple[str, str]] = set()
    for record in records:
        key = (record.repository, record.digest)
        if key in deleted:
            continue
        client.delete_manifest(record.repository, record.digest)
        deleted.add(key)
    return records


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
    scanned = repositories[:max(0, max_repositories)]
    records: list[dict[str, Any]] = []
    scanned_tag_count = 0
    visible_tag_count_total = 0
    unavailable_records: list[dict[str, Any]] = []
    for repository in scanned:
        try:
            tags = sorted(client.tags(repository))
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
        visible_tag_limit = max(0, max_tags_per_repository)
        visible_tags = tags[-visible_tag_limit:] if visible_tag_limit else []
        scanned_tag_count += len(tags)
        visible_tag_count_total += len(visible_tags)
        records.append(
            {
                "repository": repository,
                "namespace": repository.split("/", 1)[0] if "/" in repository else "",
                "available": True,
                "tag_count": len(tags),
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
    now: datetime | None = None,
) -> list[RegistryTag]:
    keep = max(0, keep_per_repository)
    cutoff = _age_cutoff(max_age_days, now=now)
    candidates: list[RegistryTag] = []
    by_repository: dict[str, list[RegistryTag]] = {}
    for record in records:
        by_repository.setdefault(record.repository, []).append(record)
    for repository_records in by_repository.values():
        ordered = sorted(
            repository_records,
            key=lambda item: _tag_sort_key(item, use_last_used_at=use_last_used_at),
            reverse=True,
        )
        protected_digests = {record.digest for record in ordered[:keep]}
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
        isinstance(item, dict) and item.get("code") == "NAME_UNKNOWN"
        for item in errors
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
        target = part[start + 1:end]
        parsed = urlparse(target)
        path = parsed.path or target
        if parsed.query:
            path = f"{path}?{parsed.query}"
        return path
    return ""
