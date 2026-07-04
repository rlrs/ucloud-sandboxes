from __future__ import annotations

from importlib import metadata


PACKAGE_NAME = "ucloud-sandboxes"
DEFAULT_INIT_VERSION = "2"

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


def service_health(service: str) -> dict[str, object]:
    return {
        "ok": True,
        "service": service,
        "version": package_version(),
    }
