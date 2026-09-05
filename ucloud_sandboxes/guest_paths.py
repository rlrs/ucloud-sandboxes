"""Linux guest paths are not paths in the controller's filesystem."""

from pathlib import PurePosixPath


def validate_guest_path(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.startswith("/"):
        raise ValueError(f"{name} must be an absolute container path.")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{name} contains unsupported control characters.")
    if ".." in PurePosixPath(value).parts:
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
    path = PurePosixPath(value)
    reserved_trees = ("/proc", "/sys", "/dev", "/run", "/.ucloud-managed")
    reserved_roots = {
        "/",
        "/etc",
        "/bin",
        "/sbin",
        "/lib",
        "/lib64",
        "/usr",
        "/var",
        "/home",
        "/root",
        "/tmp",
        "/opt",
        "/boot",
        "/.ucloud-init",
        "/.ucloud-job-init",
    }
    if value in reserved_roots or any(
        path == PurePosixPath(root) or PurePosixPath(root) in path.parents
        for root in reserved_trees
    ):
        raise ValueError("workspace_path overlaps a reserved system or runtime path.")
