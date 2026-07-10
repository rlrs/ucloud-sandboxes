from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from threading import get_ident
import time
from typing import Any, Iterable
from urllib import error, parse, request


DEFAULT_BASE_URL = "https://cloud.sdu.dk"
MAX_UCLOUD_JSON_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_UCLOUD_ERROR_PREVIEW_BYTES = 64 * 1024
MAX_UCLOUD_PAGINATION_PAGES = 10_000
MAX_UCLOUD_INVENTORY_ITEMS = 1_000_000
MAX_UCLOUD_SESSION_BYTES = 1024 * 1024


class UCloudError(RuntimeError):
    pass


class UCloudHttpError(UCloudError):
    def __init__(self, method: str, path: str, status: int, payload: object) -> None:
        self.method = method.upper()
        self.path = path
        self.status = int(status)
        self.payload = payload
        super().__init__(
            f"UCloud {self.method} {path} failed ({self.status}): {payload}"
        )


class UCloudTransportError(UCloudError):
    pass


def job_labels(payload: object) -> dict[str, str]:
    if not isinstance(payload, dict):
        return {}
    specification = payload.get("specification")
    if not isinstance(specification, dict):
        return {}
    labels = specification.get("labels")
    if not isinstance(labels, dict):
        return {}
    return {str(key): str(value) for key, value in labels.items()}


def jobs_matching_labels(
    jobs: Iterable[dict[str, Any]],
    required_labels: dict[str, str],
) -> list[dict[str, Any]]:
    required = {str(key): str(value) for key, value in required_labels.items()}
    if not required:
        return []
    return [
        job
        for job in jobs
        if isinstance(job, dict)
        and all(job_labels(job).get(key) == value for key, value in required.items())
    ]


@dataclass
class SessionState:
    base_url: str = DEFAULT_BASE_URL
    cookies: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    selected_project: str | None = None
    updated_at: str = ""

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SessionState":
        return cls(
            base_url=str(raw.get("base_url") or DEFAULT_BASE_URL),
            cookies={str(k): str(v) for k, v in dict(raw.get("cookies") or {}).items()},
            headers={str(k): str(v) for k, v in dict(raw.get("headers") or {}).items()},
            selected_project=(
                str(raw["selected_project"])
                if raw.get("selected_project") not in (None, "")
                else None
            ),
            updated_at=str(raw.get("updated_at") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "cookies": dict(self.cookies),
            "headers": dict(self.headers),
            "selected_project": self.selected_project,
            "updated_at": self.updated_at,
        }


class SessionStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> SessionState:
        if not self.path.exists():
            raise UCloudError(f"UCloud session file not found: {self.path}")
        try:
            with self.path.open("rb") as session_file:
                payload = session_file.read(MAX_UCLOUD_SESSION_BYTES + 1)
            if len(payload) > MAX_UCLOUD_SESSION_BYTES:
                raise UCloudError(f"UCloud session file is too large: {self.path}")
            raw = json.loads(payload.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UCloudError(f"Invalid UCloud session file: {self.path}") from exc
        if not isinstance(raw, dict):
            raise UCloudError(f"Invalid UCloud session file: {self.path}")
        try:
            return SessionState.from_dict(raw)
        except (TypeError, ValueError, OverflowError) as exc:
            raise UCloudError(f"Invalid UCloud session file: {self.path}") from exc

    def save(self, session: SessionState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_name(
            f".{self.path.name}.{os.getpid()}.{get_ident()}.{time.monotonic_ns()}.tmp"
        )
        payload = json.dumps(session.to_dict(), indent=2, sort_keys=True).encode(
            "utf-8"
        )
        try:
            fd = os.open(tmp_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                view = memoryview(payload)
                while view:
                    written = os.write(fd, view)
                    if written <= 0:
                        raise OSError("failed to write UCloud session file")
                    view = view[written:]
                os.fsync(fd)
            finally:
                os.close(fd)
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


class UCloudClient:
    def __init__(self, store: SessionStore) -> None:
        self.store = store
        self.session = store.load()

    def browse_jobs(
        self,
        project_id: str,
        *,
        items_per_page: int = 100,
        max_pages: int = 1,
        include_application: bool = False,
        require_complete: bool = False,
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {
            "itemsPerPage": str(items_per_page),
            "includeApplication": str(include_application).lower(),
        }
        items: list[dict[str, Any]] = []
        next_token: str | None = None
        seen_tokens: set[str] = set()
        pages = 0

        while True:
            page_params = dict(params)
            if next_token:
                page_params["next"] = next_token
            payload = self.request_json(
                "GET",
                "/api/jobs/browse",
                project_id=project_id,
                params=page_params,
            )
            if not isinstance(payload, dict):
                if require_complete:
                    raise UCloudError("Invalid jobs browse response while paginating.")
                return items
            page_items = payload.get("items")
            if require_complete and not isinstance(page_items, list):
                raise UCloudError("Jobs browse response is missing an items list.")
            if isinstance(page_items, list):
                items.extend(item for item in page_items if isinstance(item, dict))
                if len(items) > MAX_UCLOUD_INVENTORY_ITEMS:
                    raise UCloudError(
                        "Jobs browse inventory exceeded the configured safety limit."
                    )
            pages += 1
            if pages > MAX_UCLOUD_PAGINATION_PAGES:
                raise UCloudError(
                    "Jobs browse pagination exceeded the configured safety limit."
                )

            raw_next = payload.get("next")
            if (
                require_complete
                and raw_next not in (None, "")
                and not isinstance(raw_next, str)
            ):
                raise UCloudError(
                    "Jobs browse response contains an invalid next cursor."
                )
            next_token = raw_next if isinstance(raw_next, str) and raw_next else None
            if not next_token:
                return items
            if next_token in seen_tokens:
                if require_complete:
                    raise UCloudError(
                        "Jobs browse pagination repeated a cursor; refusing partial inventory."
                    )
                return items
            seen_tokens.add(next_token)
            if max_pages > 0 and pages >= max_pages:
                if require_complete:
                    raise UCloudError(
                        "Jobs browse pagination reached max_pages before completion."
                    )
                return items

    def browse_all_jobs(
        self,
        project_id: str,
        *,
        items_per_page: int = 100,
        include_application: bool = False,
    ) -> list[dict[str, Any]]:
        """Return a complete job inventory or fail instead of returning a prefix."""

        return self.browse_jobs(
            project_id,
            items_per_page=items_per_page,
            max_pages=0,
            include_application=include_application,
            require_complete=True,
        )

    def retrieve_job(
        self,
        project_id: str,
        job_id: str,
        *,
        include_updates: bool = True,
    ) -> dict[str, Any]:
        payload = self.request_json(
            "GET",
            "/api/jobs/retrieve",
            project_id=project_id,
            params={
                "id": job_id,
                "includeUpdates": str(include_updates).lower(),
            },
        )
        if not isinstance(payload, dict):
            raise UCloudError(f"Unexpected job retrieve payload for {job_id}.")
        return payload

    def submit_jobs(
        self,
        project_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        response = self.request_json(
            "POST",
            "/api/jobs",
            project_id=project_id,
            json_body=payload,
        )
        if not isinstance(response, dict):
            raise UCloudError("Unexpected jobs submit response payload.")
        return response

    def terminate_jobs(
        self,
        project_id: str,
        job_ids: list[str] | tuple[str, ...],
    ) -> dict[str, Any]:
        if not job_ids:
            return {"responses": []}
        response = self.request_json(
            "POST",
            "/api/jobs/terminate",
            project_id=project_id,
            json_body={
                "type": "bulk",
                "items": [{"id": str(job_id)} for job_id in job_ids],
            },
        )
        if not isinstance(response, dict):
            raise UCloudError("Unexpected jobs terminate response payload.")
        return response

    def open_interactive_session(
        self,
        project_id: str,
        job_id: str,
        *,
        session_type: str,
        rank: int = 0,
        port: int | None = None,
        target: str | None = None,
    ) -> dict[str, Any]:
        item: dict[str, Any] = {
            "id": str(job_id),
            "rank": int(rank),
            "sessionType": session_type,
        }
        if port is not None:
            item["port"] = int(port)
        if target:
            item["target"] = str(target)
        response = self.request_json(
            "POST",
            "/api/jobs/interactiveSession",
            project_id=project_id,
            json_body={"type": "bulk", "items": [item]},
        )
        if not isinstance(response, dict):
            raise UCloudError("Unexpected interactive session response payload.")
        return response

    def browse_ssh_keys(self, *, items_per_page: int = 250) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        next_token: str | None = None
        seen_tokens: set[str] = set()
        for _page in range(MAX_UCLOUD_PAGINATION_PAGES):
            params = {"itemsPerPage": str(items_per_page)}
            if next_token is not None:
                params["next"] = next_token
            payload = self.request_json("GET", "/api/ssh/browse", params=params)
            page_items = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(page_items, list):
                raise UCloudError("Unexpected SSH key browse response payload.")
            items.extend(item for item in page_items if isinstance(item, dict))
            if len(items) > MAX_UCLOUD_INVENTORY_ITEMS:
                raise UCloudError(
                    "SSH key inventory exceeded the configured safety limit."
                )
            raw_next = payload.get("next")
            if raw_next in (None, ""):
                return items
            if not isinstance(raw_next, str):
                raise UCloudError("SSH key browse response has an invalid next cursor.")
            if raw_next in seen_tokens:
                raise UCloudError("SSH key browse pagination repeated a cursor.")
            seen_tokens.add(raw_next)
            next_token = raw_next
        raise UCloudError("SSH key browse pagination exceeded the safety limit.")

    def create_ssh_key(self, *, title: str, key: str) -> dict[str, Any]:
        response = self.request_json(
            "POST",
            "/api/ssh",
            json_body={
                "type": "bulk",
                "items": [{"title": title, "key": key}],
            },
        )
        if not isinstance(response, dict):
            raise UCloudError("Unexpected SSH key create response payload.")
        return response

    def request_json(
        self,
        method: str,
        path: str,
        *,
        project_id: str | None = None,
        params: dict[str, str] | None = None,
        json_body: object | None = None,
    ) -> object:
        status, payload = self._request_json_once(
            method,
            path,
            project_id=project_id,
            params=params,
            json_body=json_body,
        )
        if status in {401, 403} and self._looks_auth_related(payload):
            if self.refresh():
                status, payload = self._request_json_once(
                    method,
                    path,
                    project_id=project_id,
                    params=params,
                    json_body=json_body,
                )
        if status >= 400:
            raise UCloudHttpError(method, path, status, payload)
        return payload

    def refresh(self) -> bool:
        refresh_token = self.session.cookies.get("refreshToken")
        if not refresh_token:
            return False

        url = self._url("/auth/refresh", None)
        req = request.Request(
            url,
            method="POST",
            headers={"Authorization": f"Bearer {refresh_token}"},
        )
        status, payload = self._open_json(req)
        if status >= 400 or not isinstance(payload, dict):
            return False
        access_token = payload.get("accessToken")
        csrf_token = payload.get("csrfToken")
        if not isinstance(access_token, str) or not isinstance(csrf_token, str):
            return False
        self.session.headers["Authorization"] = f"Bearer {access_token}"
        self.session.headers["X-CSRFToken"] = csrf_token
        self.session.updated_at = datetime.now(timezone.utc).isoformat()
        self.store.save(self.session)
        return True

    def _request_json_once(
        self,
        method: str,
        path: str,
        *,
        project_id: str | None,
        params: dict[str, str] | None,
        json_body: object | None,
    ) -> tuple[int, object]:
        body: bytes | None = None
        headers = dict(self.session.headers)
        if self.session.cookies:
            headers["Cookie"] = "; ".join(
                f"{name}={value}" for name, value in self.session.cookies.items()
            )
        if project_id:
            headers["Project"] = project_id
        if json_body is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(json_body).encode("utf-8")

        req = request.Request(
            self._url(path, params),
            data=body,
            method=method.upper(),
            headers=headers,
        )
        return self._open_json(req)

    def _url(self, path: str, params: dict[str, str] | None) -> str:
        base = self.session.base_url.rstrip("/")
        if not path.startswith("/"):
            path = "/" + path
        url = base + path
        if params:
            url += "?" + parse.urlencode(params)
        return url

    def _open_json(self, req: request.Request) -> tuple[int, object]:
        try:
            with request.urlopen(req, timeout=30.0) as response:
                raw = _read_bounded(
                    response,
                    MAX_UCLOUD_JSON_RESPONSE_BYTES,
                    "UCloud JSON response",
                ).decode("utf-8")
                return response.status, self._decode_json(raw)
        except error.HTTPError as exc:
            try:
                raw = _read_bounded(
                    exc,
                    MAX_UCLOUD_ERROR_PREVIEW_BYTES,
                    "UCloud error response",
                ).decode("utf-8", errors="replace")
                return exc.code, self._decode_json(raw)
            finally:
                exc.close()
        except error.URLError as exc:
            raise UCloudTransportError(f"UCloud request failed: {exc}") from exc

    @staticmethod
    def _decode_json(raw: str) -> object:
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    @staticmethod
    def _looks_auth_related(payload: object) -> bool:
        markers = ("forbidden", "unauthorized", "token", "csrf", "expired")
        if isinstance(payload, str):
            return any(marker in payload.lower() for marker in markers)
        if isinstance(payload, dict):
            bits: list[str] = []
            for key in ("why", "message", "error", "detail"):
                value = payload.get(key)
                if isinstance(value, str):
                    bits.append(value.lower())
            return any(marker in " ".join(bits) for marker in markers)
        return False


def _read_bounded(response: Any, limit: int, label: str) -> bytes:
    payload = response.read(limit + 1)
    if len(payload) > limit:
        raise UCloudTransportError(f"{label} exceeded {limit} bytes")
    return payload
