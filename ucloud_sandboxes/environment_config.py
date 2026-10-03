"""Bootstrap trust for the optional immutable image adapter."""
import base64
from dataclasses import asdict, dataclass, replace
import json
import os
import re
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
    preserve_mtimes = getattr(args, "environment_preserve_mtimes", False)
    if registry is None and key_path is None and not allowlist and not preserve_mtimes:
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
                                      registry, key, root / "scratch", preparation_subprocess=True,
                                      release_published_tag=True, preserve_mtimes=preserve_mtimes)
    return lambda spec: builder.publish_image(spec.tag, allowlist=allowlist)


_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _origin(value, name):
    from urllib.parse import urlsplit
    parsed = urlsplit(value) if isinstance(value, str) else None
    if (parsed is None or parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment or parsed.username or any(c in value for c in "\0\r\n ")):
        raise ValueError(f"immutable_environments.chunk_store.{name} must be an HTTP(S) origin")
    return parsed


@dataclass(frozen=True)
class StoreNodeConfig:
    """``chunk_store.store_node`` (C2.6): a read-through NVMe cache over S3 on
    the private network. Workers read through it with the index's read token
    and never reach S3; ``serve_index`` moves ``ucloud-chunk-index`` there too.
    """
    url: str  # How workers and the index name it, e.g. http://10.42.0.10:5091.
    listen: str  # host:port it binds.
    cache_dir: str
    cache_bytes: int
    extent_bytes: int  # Fill unit: a power of two, 1-64 MiB (64 MiB = whole packs).
    s3_concurrency: int
    serve_index: bool

    @classmethod
    def from_dict(cls, raw):
        from dataclasses import fields
        if not isinstance(raw, dict) or set(raw) != {field.name for field in fields(cls)}:
            raise ValueError("immutable_environments.chunk_store.store_node fields do not match schema")
        result = cls(**raw)
        _origin(result.url, "store_node.url")
        host, _, port = result.listen.rpartition(":") if isinstance(result.listen, str) else ("", "", "")
        if not host or not port.isdigit() or not 0 < int(port) < 65536 or any(c in host for c in "\0\r\n /"):
            raise ValueError("immutable_environments.chunk_store.store_node.listen must be host:port")
        if not isinstance(result.cache_dir, str) or not Path(result.cache_dir).is_absolute() or "\n" in result.cache_dir:
            raise ValueError("immutable_environments.chunk_store.store_node.cache_dir must be absolute")
        extent = result.extent_bytes
        for name, value, low, high in (("cache_bytes", result.cache_bytes, 1024 ** 3, 1 << 50),
                                       ("extent_bytes", extent, 1 << 20, 64 << 20),
                                       ("s3_concurrency", result.s3_concurrency, 1, 512)):
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"immutable_environments.chunk_store.store_node.{name} must be in [{low}, {high}]")
        if extent & (extent - 1) or not isinstance(result.serve_index, bool):
            raise ValueError("immutable_environments.chunk_store.store_node extent_bytes or serve_index is invalid")
        return replace(result, url=result.url.rstrip("/"))


@dataclass(frozen=True)
class ChunkStoreConfig:
    """``immutable_environments.chunk_store`` (docs/chunk-store-design.md).

    The S3 key is named by environment variable and read only by
    ``ucloud-chunk-index``, builders and the store node; workers get presigned
    URLs, or with ``store_node`` the node's URLs. Every field but
    ``store_node`` is required, as for ``snapshot_store``.
    """
    endpoint: str
    bucket: str
    region: str
    prefix: str
    access_key_id_env: str
    secret_access_key_env: str
    force_path_style: bool
    index_url: str  # How builders and workers reach the service.
    index_listen: str  # host:port the gateway service binds.
    index_database: str
    read_token_file: str
    write_token_file: str
    url_ttl_seconds: int
    mount_granularity: str  # image (merged bootstrap) or layer (stacked); S12 decides.
    nydus_image: str
    concurrent_misses: int
    store_node: StoreNodeConfig | None = None  # Phase B (C2.6); off when absent.

    @classmethod
    def from_dict(cls, raw):
        from dataclasses import fields
        names = {field.name for field in fields(cls)} - {"store_node"}
        if not isinstance(raw, dict) or set(raw) - {"store_node"} != names:
            raise ValueError("immutable_environments.chunk_store fields do not match schema")
        node = raw.get("store_node")
        result = cls(**{**raw, "store_node": None if node is None else StoreNodeConfig.from_dict(node)})
        for name in names - {"force_path_style", "url_ttl_seconds", "concurrent_misses"}:
            value = getattr(result, name)
            if not isinstance(value, str) or not value or any(c in value for c in "\0\r\n "):
                raise ValueError(f"immutable_environments.chunk_store.{name} must be a nonempty string")
        from .config import normalize_s3_endpoint
        endpoint = normalize_s3_endpoint(result.endpoint, bucket=result.bucket, region=result.region,
                                         field_name="immutable_environments.chunk_store.endpoint")
        if (not isinstance(result.force_path_style, bool) or "/" in result.bucket
                or any(part in ("", ".", "..") for part in result.prefix.strip("/").split("/"))):
            raise ValueError("immutable_environments.chunk_store has an invalid bucket, prefix or addressing")
        for name in ("access_key_id_env", "secret_access_key_env"):
            if not _ENV_NAME.fullmatch(getattr(result, name)):
                raise ValueError(f"immutable_environments.chunk_store.{name} must name an environment variable")
        for name in ("index_database", "read_token_file", "write_token_file"):
            if not Path(getattr(result, name)).is_absolute():
                raise ValueError(f"immutable_environments.chunk_store.{name} must be absolute")
        host, _, port = result.index_listen.rpartition(":")
        if (not result.index_url.startswith(("http://", "https://")) or not host or not port.isdigit()
                or not 0 < int(port) < 65536):
            raise ValueError("immutable_environments.chunk_store index_url/index_listen are invalid")
        for name, low, high in (("url_ttl_seconds", 3600, 7 * 86400), ("concurrent_misses", 1, 256)):
            value = getattr(result, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"immutable_environments.chunk_store.{name} must be an integer in [{low}, {high}]")
        if result.mount_granularity not in ("image", "layer"):
            raise ValueError("immutable_environments.chunk_store.mount_granularity must be image or layer")
        if (result.store_node is not None and result.store_node.serve_index
                and _origin(result.index_url, "index_url").hostname != _origin(result.store_node.url,
                                                                                "store_node.url").hostname):
            raise ValueError("immutable_environments.chunk_store.index_url must name the store node with serve_index")
        return replace(result, endpoint=endpoint, prefix=result.prefix.strip("/"))

    @classmethod
    def from_file(cls, path):
        """The block alone, as store-node init writes it."""
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))

    def to_dict(self):
        raw = asdict(self)
        if self.store_node is None:
            del raw["store_node"]  # Older releases reject unknown fields.
        return raw

    def credentials(self, environ=None):
        environ = os.environ if environ is None else environ
        try:
            return environ[self.access_key_id_env], environ[self.secret_access_key_env]
        except KeyError as exc:
            raise ValueError(f"chunk store credentials are missing: {exc.args[0]}") from None

    def presigner(self, environ=None):
        from .chunk_index import S3Presigner
        key, secret = self.credentials(environ)
        return S3Presigner(self.endpoint, self.bucket, self.region, key, secret, path_style=self.force_path_style)

    def object_store(self, environ=None):
        """Index-service and builder side only: holds the S3 key."""
        from .chunk_index import ChunkObjectStore
        from .storage_native_s3 import Boto3S3ObjectClient
        key, secret = self.credentials(environ)
        client = Boto3S3ObjectClient(endpoint=self.endpoint, bucket=self.bucket, region=self.region,
                                     credentials={"access_key_id": key, "secret_access_key": secret},
                                     force_path_style=self.force_path_style)
        return ChunkObjectStore(client, self.presigner(environ), self.prefix, url_seconds=self.url_ttl_seconds)


def read_token(path, *, create=False):
    """An owner-only bearer token file; ``create`` makes one when absent."""
    path = Path(path)
    if create and not path.exists():
        import secrets
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(secrets.token_hex(32) + "\n")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid not in {0, os.geteuid()}:
            raise ValueError("chunk index token files must be owner-only regular files")
        token = stream.read(4097).strip()
    if not 32 <= len(token) <= 4096:
        raise ValueError("chunk index token has an invalid length")
    return token


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
    # Builders publish layout-2 layer components (per-file mtimes kept). Set
    # only after every worker and gateway runs a release that reads layout 2.
    preserve_mtimes: bool = False
    # Off switch for attach-time metadata and startup-trace prefetch (C2.2,
    # C2.3). A backend reads it once at start; bootstrap never restarts one.
    prefetch_enabled: bool = True
    # Concurrent component attaches per worker backend. 1 keeps attach serial,
    # as before 0.8.3: a 48-way burst ran first commands 7x slower in parallel.
    attach_concurrency: int = 1
    # Chunk store M2: the gateway sets each create's environment root (from
    # image-roots.sqlite3, else the annotation). Only once every worker runs
    # 0.9.0 with a chunk store: a dispatched root needs both capabilities.
    dispatch_roots: bool = False
    # Workers share startup traces through the managed registry (plan C2.7),
    # so a node that never attached a component replays another's trace.
    shared_traces: bool = False
    # Chunk-store (RAFS) images, off by default (C2.13, design §9 M1).
    chunk_store: ChunkStoreConfig | None = None

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
        if values.get("chunk_store") is not None:
            values["chunk_store"] = ChunkStoreConfig.from_dict(values["chunk_store"])
        paths = values.get("allow_paths", [])
        if not isinstance(paths, (list, tuple)):
            raise ValueError("immutable environment allow_paths must be a list")
        values["allow_paths"] = tuple(paths)
        result = cls(**values)
        import re
        if not isinstance(result.repository, str) or not re.fullmatch(r"[a-z0-9]+(?:[._/-][a-z0-9]+)*", result.repository):
            raise ValueError("invalid immutable environment repository")
        for name in ("worker_enabled", "builder_enabled", "preserve_mtimes", "prefetch_enabled", "dispatch_roots",
                     "shared_traces"):
            if not isinstance(getattr(result, name), bool):
                raise ValueError(f"immutable environment {name} must be boolean")
        if type(result.attach_concurrency) is not int or not 1 <= result.attach_concurrency <= 256:
            raise ValueError("immutable environment attach_concurrency must be an integer from 1 to 256")
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

    def to_dict(self):
        # Older releases reject unknown fields: write the switch only once set,
        # so a release rollback still reads configs rendered with it off.
        raw = asdict(self)
        if not self.preserve_mtimes:
            del raw["preserve_mtimes"]
        if self.chunk_store is None:
            del raw["chunk_store"]
        else:
            raw["chunk_store"] = self.chunk_store.to_dict()
        return raw


def environment_registry_from_deployment(config):
    selected = config.immutable_environments
    if selected is None:
        return None
    return configured_environment_registry(config.registry_url, selected.repository, selected.trusted_keys_file)
