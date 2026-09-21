from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import unquote, urlparse


@dataclass(frozen=True)
class SandboxHttpRoute:
    """Canonical semantics for one supported sandbox HTTP operation."""

    action: str
    sandbox_id: str
    job_id: str = ""
    stream: str = ""
    sdk_public: bool = False
    wakes: bool = False


def match_sandbox_http_route(method: str, path: str) -> SandboxHttpRoute | None:
    """Parse the shared gateway/node sandbox API without suffix heuristics.

    Internal migration and publication routes deliberately remain outside this
    contract. They are node-control operations and never participate in public
    SDK authorization or implicit wake behavior.
    """

    method = method.upper()
    normalized = urlparse(path).path
    prefix = "/v1/sandboxes/"
    if not normalized.startswith(prefix):
        return None
    raw_parts = normalized[len(prefix) :].split("/")
    if not raw_parts or any(not item for item in raw_parts):
        return None
    parts = [unquote(item) for item in raw_parts]
    if any("/" in item for item in parts):
        return None
    sandbox_id = parts[0]
    suffix = parts[1:]

    if not suffix and method == "DELETE":
        return SandboxHttpRoute("delete", sandbox_id, sdk_public=True)
    if suffix == ["environment"] and method == "GET":
        return SandboxHttpRoute("environment", sandbox_id, sdk_public=True)
    if suffix == ["files"] and method in {"GET", "PUT"}:
        return SandboxHttpRoute("files", sandbox_id, sdk_public=True, wakes=True)
    if suffix == ["ssh"] and method == "GET":
        return SandboxHttpRoute("ssh", sandbox_id, sdk_public=True, wakes=True)
    if suffix == ["exec"] and method == "POST":
        return SandboxHttpRoute("exec", sandbox_id, sdk_public=True, wakes=True)
    if suffix == ["jobs"] and method == "POST":
        return SandboxHttpRoute("job_create", sandbox_id, sdk_public=True, wakes=True)
    if len(suffix) == 2 and suffix[0] == "jobs" and method == "GET":
        return SandboxHttpRoute(
            "job_status", sandbox_id, job_id=suffix[1], sdk_public=True
        )
    if (
        len(suffix) == 3
        and suffix[0] == "jobs"
        and suffix[2] == "signal"
        and method == "POST"
    ):
        return SandboxHttpRoute(
            "job_signal",
            sandbox_id,
            job_id=suffix[1],
            sdk_public=True,
            wakes=True,
        )
    if (
        len(suffix) == 4
        and suffix[0] == "jobs"
        and suffix[2] == "logs"
        and suffix[3] in {"stdout", "stderr"}
        and method == "GET"
    ):
        return SandboxHttpRoute(
            "job_logs",
            sandbox_id,
            job_id=suffix[1],
            stream=suffix[3],
            sdk_public=True,
            wakes=True,
        )
    if suffix == ["park"] and method == "POST":
        return SandboxHttpRoute("park", sandbox_id)
    if suffix == ["wake"] and method == "POST":
        return SandboxHttpRoute("wake", sandbox_id, wakes=True)
    return None
