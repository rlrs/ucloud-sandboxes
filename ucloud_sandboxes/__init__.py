"""Provider-aware gVisor sandbox control plane for UCloud."""

from importlib.metadata import PackageNotFoundError, version


try:
    __version__ = version("ucloud-sandboxes")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = ["__version__"]
