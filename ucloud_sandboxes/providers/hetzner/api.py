"""Small synchronous client for the Hetzner Cloud API."""

from __future__ import annotations

import json
import os
from typing import Any
from urllib import error, parse, request

from .config import (
    DEFAULT_HETZNER_API_BASE_URL,
    DEFAULT_HETZNER_API_TOKEN_ENV,
)


MAX_HETZNER_JSON_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_HETZNER_ERROR_PREVIEW_BYTES = 64 * 1024
MAX_HETZNER_PAGINATION_PAGES = 10_000
MAX_HETZNER_INVENTORY_ITEMS = 1_000_000


class HetznerError(RuntimeError):
    pass


class HetznerHttpError(HetznerError):
    def __init__(self, method: str, path: str, status: int, payload: object) -> None:
        self.method = method.upper()
        self.path = path
        self.status = int(status)
        self.payload = payload
        api_error = payload.get("error") if isinstance(payload, dict) else None
        code = api_error.get("code") if isinstance(api_error, dict) else None
        message = api_error.get("message") if isinstance(api_error, dict) else None
        detail = ": ".join(str(item) for item in (code, message) if item)
        suffix = f": {detail}" if detail else ""
        super().__init__(f"Hetzner {self.method} {path} failed ({self.status}){suffix}")


class HetznerTransportError(HetznerError):
    pass


class HetznerClient:
    def __init__(
        self,
        *,
        api_token_env: str = DEFAULT_HETZNER_API_TOKEN_ENV,
        base_url: str = DEFAULT_HETZNER_API_BASE_URL,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.api_token_env = api_token_env
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    @property
    def api_token(self) -> str:
        token = os.environ.get(self.api_token_env, "").strip()
        if not token:
            raise HetznerError(
                f"Hetzner API token environment variable {self.api_token_env!r} "
                "is not set"
            )
        return token

    def list_servers(
        self,
        *,
        label_selector: str,
        per_page: int = 50,
    ) -> list[dict[str, Any]]:
        servers: list[dict[str, Any]] = []
        page = 1
        seen_pages: set[int] = set()
        for _ in range(MAX_HETZNER_PAGINATION_PAGES):
            if page in seen_pages:
                raise HetznerError(
                    "Hetzner server pagination repeated a page; refusing partial inventory"
                )
            seen_pages.add(page)
            payload = self.request_json(
                "GET",
                "/servers",
                params={
                    "label_selector": label_selector,
                    "page": page,
                    "per_page": per_page,
                },
            )
            if not isinstance(payload, dict):
                raise HetznerError("Hetzner server list response is not an object")
            page_servers = payload.get("servers")
            if not isinstance(page_servers, list):
                raise HetznerError("Hetzner server list response has no servers list")
            servers.extend(item for item in page_servers if isinstance(item, dict))
            if len(servers) > MAX_HETZNER_INVENTORY_ITEMS:
                raise HetznerError(
                    "Hetzner server inventory exceeded the configured safety limit"
                )
            next_page = _next_page(payload)
            if next_page is None:
                return servers
            page = next_page
        raise HetznerError("Hetzner server pagination exceeded the safety limit")

    def retrieve_server(self, server_id: str) -> dict[str, Any]:
        payload = self.request_json("GET", _server_path(server_id))
        if not isinstance(payload, dict) or not isinstance(payload.get("server"), dict):
            raise HetznerError(f"Hetzner server response is invalid for {server_id}")
        return payload["server"]

    def create_server(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.request_json("POST", "/servers", json_body=payload)
        if not isinstance(response, dict):
            raise HetznerError("Hetzner server create response is not an object")
        return response

    def delete_server(self, server_id: str) -> dict[str, Any]:
        response = self.request_json("DELETE", _server_path(server_id))
        if response is None:
            return {}
        if not isinstance(response, dict):
            raise HetznerError("Hetzner server delete response is not an object")
        return response

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, object] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> object:
        normalized_path = "/" + path.lstrip("/")
        url = self.base_url + normalized_path
        if params:
            url += "?" + parse.urlencode(params, doseq=True)
        body = None
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "application/json",
            "User-Agent": "ucloud-sandboxes/hetzner-provider",
        }
        if json_body is not None:
            body = json.dumps(json_body, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = request.Request(url, data=body, headers=headers, method=method.upper())
        return self._open_json(req, method=method, path=normalized_path)

    def _open_json(
        self,
        req: request.Request,
        *,
        method: str,
        path: str,
    ) -> object:
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                payload = response.read(MAX_HETZNER_JSON_RESPONSE_BYTES + 1)
        except error.HTTPError as exc:
            payload = exc.read(MAX_HETZNER_ERROR_PREVIEW_BYTES + 1)
            if len(payload) > MAX_HETZNER_ERROR_PREVIEW_BYTES:
                raise HetznerTransportError(
                    "Hetzner HTTP error response exceeded the diagnostic limit"
                ) from exc
            raise HetznerHttpError(
                method,
                path,
                exc.code,
                _decode_json_or_text(payload),
            ) from exc
        except (error.URLError, TimeoutError, OSError) as exc:
            raise HetznerTransportError(
                f"Hetzner {method.upper()} {path} transport failed: {exc}"
            ) from exc
        if len(payload) > MAX_HETZNER_JSON_RESPONSE_BYTES:
            raise HetznerTransportError(
                "Hetzner JSON response exceeded the configured size limit"
            )
        if not payload:
            return None
        try:
            return json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HetznerTransportError(
                f"Hetzner {method.upper()} {path} returned invalid JSON"
            ) from exc


def _next_page(payload: dict[str, Any]) -> int | None:
    meta = payload.get("meta")
    pagination = meta.get("pagination") if isinstance(meta, dict) else None
    if not isinstance(pagination, dict):
        return None
    value = pagination.get("next_page")
    if value in (None, 0):
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise HetznerError("Hetzner server pagination returned an invalid next page")
    return value


def _decode_json_or_text(payload: bytes) -> object:
    if not payload:
        return {}
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return payload.decode("utf-8", errors="replace")


def _server_path(server_id: str) -> str:
    value = str(server_id).strip()
    if not value.isdigit() or int(value) <= 0:
        raise HetznerError(f"invalid Hetzner server id: {server_id!r}")
    return f"/servers/{parse.quote(value, safe='')}"
