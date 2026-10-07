"""Public gateway credential checks, independent of the HTTP handler."""

from __future__ import annotations

import hmac
from typing import Any
from urllib.parse import unquote

from ..http_contract import match_sandbox_http_route


def _token_matches(headers: Any, expected: str, *, allow_ucloud_sandbox_header: bool) -> bool:
    authorization = headers.get("Authorization") or ""
    prefix = "Bearer "
    bearer = authorization[len(prefix) :] if authorization.startswith(prefix) else ""
    if bearer and hmac.compare_digest(bearer, expected):
        return True
    if allow_ucloud_sandbox_header:
        public_link_token = headers.get("X-UCloud-Sandbox-Token") or ""
        if public_link_token and hmac.compare_digest(public_link_token, expected):
            return True
    return False


def _is_sdk_api_request(method: str, path: str) -> bool:
    """Return whether the least-privileged public SDK key may use a route."""

    method = method.upper()
    exact_routes = {
        ("GET", "/v1/sandboxes"),
        ("POST", "/v1/sandboxes"),
        ("GET", "/v1/capacity/prepare"),
        ("POST", "/v1/capacity/prepare"),
        ("GET", "/v1/builders/prepare"),
        ("POST", "/v1/builders/prepare"),
        ("GET", "/v1/images"),
        ("GET", "/v1/images/builds"),
        ("POST", "/v1/images/build"),
        ("POST", "/v1/images/pull"),
        ("POST", "/v1/images/ensure"),
        ("POST", "/v1/image-recipes"),
        ("POST", "/v1/sandboxes:batch"),
    }
    if (method, path) in exact_routes:
        return True
    for prefix, methods in (
        ("/v1/capacity/prepare/", {"DELETE"}),
        ("/v1/builders/prepare/", {"DELETE"}),
        ("/v1/images/builds/", {"GET"}),
        ("/v1/image-contexts/", {"GET", "PUT"}),
        ("/v1/sandboxes:batch/", {"GET", "DELETE"}),
    ):
        if method in methods and _single_encoded_path_segment(path, prefix):
            return True

    sandbox_route = match_sandbox_http_route(method, path)
    if sandbox_route is not None:
        return sandbox_route.sdk_public

    exec_parts = _encoded_path_parts(path, "/v1/exec/")
    if exec_parts is None:
        return False
    if len(exec_parts) == 1:
        return method == "GET"
    if len(exec_parts) != 2:
        return False
    if exec_parts[1] == "events":
        return method == "GET"
    if exec_parts[1] in {"stdin", "close-stdin", "signal"}:
        return method == "POST"
    return False


def _single_encoded_path_segment(path: str, prefix: str) -> bool:
    parts = _encoded_path_parts(path, prefix)
    return parts is not None and len(parts) == 1


def _encoded_path_parts(path: str, prefix: str) -> list[str] | None:
    if not path.startswith(prefix):
        return None
    raw = path[len(prefix) :]
    if not raw:
        return None
    parts = raw.split("/")
    if any(not part or "/" in unquote(part) for part in parts):
        return None
    return parts
