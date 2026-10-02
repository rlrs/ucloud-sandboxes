"""Exec session identifiers that carry their own signed worker route.

The gateway gives the worker an authenticated prefix when it forwards an exec
start. A capable worker names the session ``<prefix>.<random>``; the gateway
can then route every later poll, stdin, signal and close request by verifying
the prefix, without a durable exec-route row or a database read. Workers that
ignore the prefix keep minting ``exec-<uuid>`` names, which continue to use the
durable route table.

The prefix binds the sandbox incarnation and worker job. It never redirects an
accepted command: when that worker is gone the session is reported lost, as
for routed sessions. Rotating the gateway credential invalidates outstanding
prefixes; their sessions then read as unknown.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re

EXEC_SESSION_PREFIX_HEADER = "X-UCloud-Exec-Session-Prefix"

_VERSION = "xr1"
_MAC_BYTES = 16
_KEY_CONTEXT = b"ucloud-sandboxes exec session route v1"
_MAX_PREFIX_LENGTH = 768
PREFIX_RE = re.compile(r"xr1\.[A-Za-z0-9_-]{1,700}\.[A-Za-z0-9_-]{22}")
_SESSION_RE = re.compile(r"(xr1\.[A-Za-z0-9_-]{1,700}\.[A-Za-z0-9_-]{22})\.[0-9a-f]{32}")


@dataclass(frozen=True)
class SignedExecRoute:
    sandbox_id: str
    sandbox_generation: int
    node_id: str
    job_id: str
    issued_at: int

    @property
    def issued_at_iso(self) -> str:
        return datetime.fromtimestamp(self.issued_at, timezone.utc).isoformat()


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def valid_session_prefix(prefix: str) -> bool:
    """Worker-side shape check; workers cannot verify the gateway signature."""

    return len(prefix) <= _MAX_PREFIX_LENGTH and PREFIX_RE.fullmatch(prefix) is not None


class ExecSessionRoutes:
    def __init__(self, secret: str) -> None:
        if not isinstance(secret, str) or not secret.strip():
            raise ValueError("exec session routes require a gateway secret")
        self._key = hmac.new(
            secret.strip().encode("utf-8"), _KEY_CONTEXT, hashlib.sha256
        ).digest()

    def _mac(self, payload: str) -> str:
        digest = hmac.new(
            self._key, f"{_VERSION}.{payload}".encode("ascii"), hashlib.sha256
        ).digest()
        return _b64(digest[:_MAC_BYTES])

    def prefix(self, route: SignedExecRoute) -> str:
        payload = _b64(
            json.dumps(
                [
                    route.sandbox_id,
                    route.sandbox_generation,
                    route.node_id,
                    route.job_id,
                    route.issued_at,
                ],
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        )
        prefix = f"{_VERSION}.{payload}.{self._mac(payload)}"
        if not valid_session_prefix(prefix):
            raise ValueError("exec route identity is too long for a session prefix")
        return prefix

    def decode(self, session_id: str) -> SignedExecRoute | None:
        """Return the signed route, or None for routed or unauthentic names."""

        match = _SESSION_RE.fullmatch(session_id)
        if match is None:
            return None
        _, payload, mac = match.group(1).split(".")
        if not hmac.compare_digest(mac, self._mac(payload)):
            return None
        try:
            values = json.loads(_unb64(payload).decode("utf-8"))
            sandbox_id, generation, node_id, job_id, issued_at = values
        except (TypeError, ValueError, UnicodeDecodeError):
            return None
        if not (
            all(isinstance(item, str) and item for item in (sandbox_id, node_id, job_id))
            and all(
                isinstance(item, int) and not isinstance(item, bool) and item > 0
                for item in (generation, issued_at)
            )
        ):
            return None
        return SignedExecRoute(sandbox_id, generation, node_id, job_id, issued_at)
