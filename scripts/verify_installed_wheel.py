"""Smoke-check the installed distribution without importing the source tree."""

from importlib.metadata import version
from importlib.resources import files

from ucloud_sandboxes.cli import build_parser
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.deployment import package_version


def main() -> None:
    assert package_version() == version("ucloud-sandboxes")
    raw_config = DeploymentConfig.default(scope_id="wheel-smoke-project").to_dict()
    raw_config["deployment_id"] = "wheel-smoke"
    config = DeploymentConfig.from_dict(raw_config)
    assert config.to_dict()["deployment_id"] == "wheel-smoke"
    assert build_parser().prog
    unit = files("ucloud_sandboxes").joinpath("systemd/ucloud-sandbox-gateway.service")
    assert unit.is_file(), "systemd units are missing from the wheel"


if __name__ == "__main__":
    main()
