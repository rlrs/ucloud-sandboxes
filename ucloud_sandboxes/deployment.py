from __future__ import annotations

from importlib import metadata


PACKAGE_NAME = "ucloud-sandboxes"
DEFAULT_INIT_VERSION = "2"
# Node API compatibility is intentionally independent of the gateway package
# patch version. Bump this floor only when a node-agent protocol change cannot
# be handled through capability negotiation or backwards-compatible parsing.
MIN_COMPATIBLE_AGENT_VERSION = "0.3.42"

NODE_LABEL = "ucloud-sandboxes/node"
GATEWAY_LABEL = "ucloud-sandboxes/gateway"
BUILDER_LABEL = "ucloud-sandboxes/builder"
RECONCILE_LABEL = "ucloud-sandboxes/reconcile"
RECONCILE_CYCLE_LABEL = "ucloud-sandboxes/reconcile-cycle"
CREATE_INDEX_LABEL = "ucloud-sandboxes/create-index"
DEPLOYMENT_LABEL = "ucloud-sandboxes/deployment"
AGENT_VERSION_LABEL = "ucloud-sandboxes/agent-version"
INIT_VERSION_LABEL = "ucloud-sandboxes/init-version"


def package_version() -> str:
    try:
        return metadata.version(PACKAGE_NAME)
    except metadata.PackageNotFoundError:
        from . import __version__

        return __version__


def agent_version_is_compatible(agent_version: str, *, expected: str | None = None) -> bool:
    expected_version = (expected or package_version()).strip()
    candidate = agent_version.strip()
    if not candidate:
        return False
    if candidate == expected_version:
        return True
    parsed_candidate = _release_version(candidate)
    parsed_expected = _release_version(expected_version)
    parsed_minimum = _release_version(MIN_COMPATIBLE_AGENT_VERSION)
    if (
        parsed_candidate is None
        or parsed_expected is None
        or parsed_minimum is None
    ):
        return False
    return bool(
        parsed_candidate[:2] == parsed_expected[:2]
        and parsed_minimum <= parsed_candidate <= parsed_expected
    )


def _release_version(value: str) -> tuple[int, int, int] | None:
    parts = value.split(".")
    if len(parts) != 3 or any(not part.isdigit() for part in parts):
        return None
    return int(parts[0]), int(parts[1]), int(parts[2])


def service_health(service: str) -> dict[str, object]:
    return {
        "ok": True,
        "service": service,
        "version": package_version(),
    }
