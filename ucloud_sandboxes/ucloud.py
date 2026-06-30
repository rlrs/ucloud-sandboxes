from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from urllib import error, parse, request


DEFAULT_BASE_URL = "https://cloud.sdu.dk"


class UCloudError(RuntimeError):
    pass


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
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise UCloudError(f"Invalid UCloud session file: {self.path}")
        return SessionState.from_dict(raw)

    def save(self, session: SessionState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(session.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        tmp_path.chmod(0o600)
        tmp_path.replace(self.path)


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
            page_items = payload.get("items") if isinstance(payload, dict) else None
            if isinstance(page_items, list):
                items.extend(item for item in page_items if isinstance(item, dict))
            pages += 1

            raw_next = payload.get("next") if isinstance(payload, dict) else None
            next_token = raw_next if isinstance(raw_next, str) and raw_next else None
            if not next_token:
                return items
            if next_token in seen_tokens:
                return items
            seen_tokens.add(next_token)
            if max_pages > 0 and pages >= max_pages:
                return items

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
        target: str | None = None,
    ) -> dict[str, Any]:
        item: dict[str, Any] = {
            "id": str(job_id),
            "rank": int(rank),
            "sessionType": session_type,
        }
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
        payload = self.request_json(
            "GET",
            "/api/ssh/browse",
            params={"itemsPerPage": str(items_per_page)},
        )
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            raise UCloudError("Unexpected SSH key browse response payload.")
        return [item for item in items if isinstance(item, dict)]

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
            raise UCloudError(f"UCloud {method.upper()} {path} failed ({status}): {payload}")
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
                raw = response.read().decode("utf-8")
                return response.status, self._decode_json(raw)
        except error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            return exc.code, self._decode_json(raw)
        except error.URLError as exc:
            raise UCloudError(f"UCloud request failed: {exc}") from exc

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
