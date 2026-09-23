"""Bootstrap trust for the optional immutable image adapter."""
import base64
from dataclasses import dataclass
import json
import os
from pathlib import Path
import stat

from .managed_registry import RegistryClient


def load_trusted_keys(path):
    from .environment_artifact import content_digest
    descriptor = os.open(Path(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid not in {0, os.geteuid()} or info.st_mode & 0o022:
            raise ValueError("environment producer trust file must be owned and not writable by others")
        data = stream.read(64 * 1024 + 1)
    if len(data) > 64 * 1024:
        raise ValueError("environment producer trust file exceeds its bound")
    raw = json.loads(data)
    if not isinstance(raw, dict) or not raw:
        raise ValueError("environment producer trust must be a nonempty key mapping")
    keys = {identity: base64.b64decode(value, validate=True) for identity, value in raw.items()}
    if any(len(key) != 32 or content_digest(key) != identity for identity, key in keys.items()):
        raise ValueError("environment producer key identity mismatch")
    return keys


def configured_environment_registry(url, repository, trusted_keys):
    if not any((url, repository, trusted_keys)):
        return None
    if not all((url, repository, trusted_keys)):
        raise ValueError("environment registry URL, repository and trusted producer keys must be configured together")
    from .environment_artifact import EnvironmentArtifactRegistry
    return EnvironmentArtifactRegistry(RegistryClient(url), repository, load_trusted_keys(trusted_keys))


def add_environment_registry_args(parser):
    parser.add_argument("--environment-registry-url", default="")
    parser.add_argument("--environment-registry-repository", default="")
    parser.add_argument("--environment-trusted-keys", type=Path)


def environment_registry_from_args(args):
    return configured_environment_registry(getattr(args, "environment_registry_url", ""),
        getattr(args, "environment_registry_repository", ""), getattr(args, "environment_trusted_keys", None))


def environment_publisher_from_args(args):
    registry = environment_registry_from_args(args)
    key_path = getattr(args, "environment_signing_key", None)
    allowlist = tuple(getattr(args, "environment_allow_path", ()) or ())
    if registry is None and key_path is None and not allowlist:
        return None
    if registry is None or key_path is None or not allowlist:
        raise ValueError("environment builder requires registry trust, a signing key, and an explicit immutable path allowlist")
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from .environment_builder import FreshEnvironmentBuilder
    from .image_rootfs import DockerOverlay2RootfsStore
    descriptor = os.open(key_path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("environment signing key must be a private owned regular file")
        payload = stream.read(16 * 1024 + 1)
    if len(payload) > 16 * 1024:
        raise ValueError("environment signing key is too large")
    key = load_pem_private_key(payload, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("environment signing requires an Ed25519 key")
    root = args.image_file.absolute().parent / "environment-build"
    builder = FreshEnvironmentBuilder(DockerOverlay2RootfsStore(root / "images", docker_binary=args.docker_binary),
                                      registry, key, root / "scratch")
    return lambda spec: builder.publish_image(spec.tag, allowlist=allowlist)


@dataclass(frozen=True)
class EnvironmentDeploymentConfig:
    """Opt-in fleet adapter; paths refer to owned controller-side key files."""
    trusted_keys_file: str
    signing_key_file: str = ""
    repository: str = "environments"
    worker_enabled: bool = False
    builder_enabled: bool = False
    allow_paths: tuple[str, ...] = ()
    cache_bytes: int = 1024 ** 3

    @classmethod
    def from_dict(cls, raw):
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise ValueError("immutable_environments must be an object")
        from dataclasses import fields
        if set(raw) - {field.name for field in fields(cls)} or "trusted_keys_file" not in raw:
            raise ValueError("invalid immutable_environments fields")
        values = dict(raw)
        paths = values.get("allow_paths", [])
        if not isinstance(paths, (list, tuple)):
            raise ValueError("immutable environment allow_paths must be a list")
        values["allow_paths"] = tuple(paths)
        result = cls(**values)
        import re
        if not isinstance(result.repository, str) or not re.fullmatch(r"[a-z0-9]+(?:[._/-][a-z0-9]+)*", result.repository):
            raise ValueError("invalid immutable environment repository")
        for name in ("worker_enabled", "builder_enabled"):
            if not isinstance(getattr(result, name), bool):
                raise ValueError(f"immutable environment {name} must be boolean")
        for name in ("trusted_keys_file", "signing_key_file"):
            value = getattr(result, name)
            if not isinstance(value, str) or any(c in value for c in "\0\r\n") or (value and not Path(value).is_absolute()):
                raise ValueError(f"immutable environment {name} must be an absolute path")
        if not result.trusted_keys_file:
            raise ValueError("immutable environment producer trust is required")
        if isinstance(result.cache_bytes, bool) or not isinstance(result.cache_bytes, int) or result.cache_bytes < 256 * 1024:
            raise ValueError("immutable environment cache_bytes must fit at least one chunk")
        for value in result.allow_paths:
            if (not isinstance(value, str) or not value or Path(value).is_absolute()
                    or any(part in {"", ".", ".."} for part in value.split("/")) or any(c in value for c in "\0\r\n")):
                raise ValueError("immutable environment allow_paths must be clean relative paths")
        if result.builder_enabled and (not result.signing_key_file or not result.allow_paths):
            raise ValueError("immutable environment builder requires signing_key_file and allow_paths")
        return result


def environment_registry_from_deployment(config):
    selected = config.immutable_environments
    if selected is None:
        return None
    return configured_environment_registry(config.registry_url, selected.repository, selected.trusted_keys_file)
