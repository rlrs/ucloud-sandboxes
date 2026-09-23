"""Linux guest paths are not paths in the controller's filesystem."""

import re


_CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f]")
_RESERVED_WORKSPACE_TREES = ("/proc", "/sys", "/dev", "/run", "/.ucloud-managed")
_RESERVED_WORKSPACE_ROOTS = frozenset({
    "/", "/etc", "/bin", "/sbin", "/lib", "/lib64", "/usr", "/var",
    "/home", "/root", "/tmp", "/opt", "/boot", "/.ucloud-init", "/.ucloud-job-init",
})


def validate_guest_path(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.startswith("/"):
        raise ValueError(f"{name} must be an absolute container path.")
    if _CONTROL_CHARACTERS.search(value):
        raise ValueError(f"{name} contains unsupported control characters.")
    if ".." in value.split("/"):
        raise ValueError(f"{name} cannot contain '..'.")


def validate_setup_path(name: str, value: str) -> None:
    validate_guest_path(name, value)
    if value != "/" and any(part in {"", "."} for part in value[1:].split("/")):
        raise ValueError(f"{name} must be a canonical absolute path.")
    # These paths are serialized into the existing colon-delimited bootstrap
    # environment. File/cwd APIs do not have that transport restriction.
    if ":" in value or "," in value:
        raise ValueError(f"{name} contains unsupported delimiters.")


def validate_workspace_path(value: str) -> None:
    validate_setup_path("workspace_path", value)
    # Setup paths are already canonical. Component-boundary prefixes express
    # the guest-only ancestry policy without allocating controller path objects.
    if value in _RESERVED_WORKSPACE_ROOTS or any(
        value == root or value.startswith(root + "/")
        for root in _RESERVED_WORKSPACE_TREES
    ):
        raise ValueError("workspace_path overlaps a reserved system or runtime path.")
