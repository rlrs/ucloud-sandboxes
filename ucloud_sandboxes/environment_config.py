"""Bootstrap trust for the optional immutable image adapter."""
import base64
from dataclasses import asdict, dataclass, replace
import json
import os
import re
from pathlib import Path
import stat

from .managed_registry import RegistryClient

# Idle image compositions stay mounted until attached components exceed this
# share of the node's block devices (768 of the 1,024 VM init loads).
DEFAULT_DEVICE_BUDGET_PERCENT = 75


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


def load_signing_key(key_path):
    """The Ed25519 environment signing key: a private, owned regular file."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
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
    return key


def environment_publisher_from_args(args):
    registry = environment_registry_from_args(args)
    key_path = getattr(args, "environment_signing_key", None)
    if getattr(args, "environment_format", "erofs") == "rafs":
        return rafs_publisher_from_args(args, registry, key_path)
    allowlist = tuple(getattr(args, "environment_allow_path", ()) or ())
    preserve_mtimes = getattr(args, "environment_preserve_mtimes", False)
    if registry is None and key_path is None and not allowlist and not preserve_mtimes:
        return None
    if registry is None or key_path is None or not allowlist:
        raise ValueError("environment builder requires registry trust, a signing key, and an explicit immutable path allowlist")
    from .environment_builder import FreshEnvironmentBuilder
    from .image_rootfs import DockerOverlay2RootfsStore
    key = load_signing_key(key_path)
    root = args.image_file.absolute().parent / "environment-build"
    builder = FreshEnvironmentBuilder(DockerOverlay2RootfsStore(root / "images", docker_binary=args.docker_binary),
                                      registry, key, root / "scratch", preparation_subprocess=True,
                                      release_published_tag=True, preserve_mtimes=preserve_mtimes)
    return lambda spec: builder.publish_image(spec.tag, allowlist=allowlist)



def rafs_publisher_from_args(args, registry, key_path):
    """builder_format "rafs": builds end in the chunk store (chunk_convert.rafs_build_publisher).
    Credentials and the pinned nydus-image are checked here, not at the first build."""
    import hashlib
    from .chunk_convert import rafs_build_publisher
    from .chunk_index import ChunkIndexClient
    config, token = getattr(args, "chunk_store_config", None), getattr(args, "chunk_index_token_file", None)
    if registry is None or key_path is None or config is None or token is None:
        raise ValueError("--environment-format rafs needs registry trust, a signing key, "
                         "--chunk-store-config and --chunk-index-token-file")
    store = ChunkStoreConfig.from_file(config)
    store.credentials()
    with open(store.nydus_image, "rb") as binary:
        if store.nydus_image_sha256 is None or hashlib.sha256(binary.read()).hexdigest() != store.nydus_image_sha256:
            raise ValueError(f"{store.nydus_image} is not the nydus-image chunk_store.nydus_image_sha256 pins")
    index = ChunkIndexClient(store.index_url, read_token(token).decode())
    return rafs_build_publisher(registry, store, index, load_signing_key(key_path),
                                args.image_file.absolute().parent / "environment-build" / "rafs")

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
    # A full replica of the S3 prefix, not a cache: it mirrors every object,
    # never evicts, and refuses fills past cache_bytes (S3 stays the permanent
    # store; the deduplicated corpus fits). Off: a read-through LRU cache.
    replica: bool = False
    mirror_seconds: int = 600
    # A block device holding cache_dir and the index (a Hetzner Volume,
    # /dev/disk/by-id/...), so the server can be replaced or resized without
    # refilling from S3. Store init mounts it; it must already be ext4.
    data_device: str | None = None
    # ucloud-chunk-serve (runtime/chunk_serve, Go) pinned by sha256: it holds
    # ``listen`` and answers reads from resident extents on every core; this
    # node then answers on 127.0.0.1 at the same port, behind it.
    native_server_sha256: str | None = None
    # The build package cache on this node (ucloud_sandboxes.package_cache): an
    # allowlisted, caching HTTP proxy for package mirrors; builds use it as http_proxy.
    package_cache: object = None

    @classmethod
    def from_dict(cls, raw):
        from dataclasses import fields
        names = {field.name for field in fields(cls)}
        if not isinstance(raw, dict) or not names - {"replica", "mirror_seconds", "data_device", "native_server_sha256",
                                                     "package_cache"} <= set(raw) <= names:
            raise ValueError("immutable_environments.chunk_store.store_node fields do not match schema")
        result = cls(**raw)
        if result.package_cache is not None:
            from .package_cache import PackageCacheConfig
            result = replace(result, package_cache=PackageCacheConfig.from_dict(result.package_cache))
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
        if extent & (extent - 1) or not isinstance(result.serve_index, bool) or not isinstance(result.replica, bool):
            raise ValueError("immutable_environments.chunk_store.store_node extent_bytes, serve_index or replica "
                             "is invalid")
        if type(result.mirror_seconds) is not int or not 60 <= result.mirror_seconds <= 86400:
            raise ValueError("immutable_environments.chunk_store.store_node.mirror_seconds must be in [60, 86400]")
        if result.data_device is not None and not (isinstance(result.data_device, str)
                                                   and re.fullmatch(r"/dev/disk/by-id/[A-Za-z0-9._:-]+",
                                                                    result.data_device)):
            raise ValueError("immutable_environments.chunk_store.store_node.data_device must be a /dev/disk/by-id/ path")
        if result.native_server_sha256 is not None and (
                not isinstance(result.native_server_sha256, str)
                or not re.fullmatch(r"[0-9a-f]{64}", result.native_server_sha256)
                or host in ("0.0.0.0", "::", "[::]", "127.0.0.1", "localhost")):
            raise ValueError("immutable_environments.chunk_store.store_node.native_server_sha256 must be a sha256, "
                             "with listen on a specific non-loopback address")
        return replace(result, url=result.url.rstrip("/"))


# nydusd as the node bundle ships it (runtime/nydusd/build_pinned.sh): v2.4.5,
# with the experimental block-nbd export no release binary carries.
PINNED_NYDUS_COMMIT = "e3190057422fee17f594bb3a5c10741b45dac6ce"
NYDUSD_FEATURES = ("block-nbd",)
NYDUSD_INSTALL_PATH = "/usr/local/libexec/ucloud-sandboxes/nydusd"


@dataclass(frozen=True)
class NydusdConfig:
    """``chunk_store.nydusd`` (C2.1, docs/benchmarks/nydusd-spike-2026-10-03):
    workers serve RAFS images with stock nydusd v2.4.5 (built with
    ``block-nbd``) from the store node's virtual blobs. The backend refuses a
    binary whose sha256 differs."""
    path: str
    sha256: str

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) != {"path", "sha256"}:
            raise ValueError("immutable_environments.chunk_store.nydusd fields do not match schema")
        result = cls(**raw)
        if (not isinstance(result.path, str) or not Path(result.path).is_absolute()
                or any(c in result.path for c in "\0\r\n '\"")
                or not isinstance(result.sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", result.sha256)):
            raise ValueError("immutable_environments.chunk_store.nydusd needs an absolute path and a sha256")
        return result


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
    nydusd: NydusdConfig | None = None  # C2.1: needs the store node's virtual blobs; off when absent.
    # The nydus-image at ``nydus_image``, pinned: builders with builder_format
    # "rafs" install it from their bundle and refuse any other.
    nydus_image_sha256: str | None = None

    @classmethod
    def from_dict(cls, raw):
        from dataclasses import fields
        optional = {"store_node", "nydusd", "nydus_image_sha256"}
        names = {field.name for field in fields(cls)} - optional
        if not isinstance(raw, dict) or set(raw) - optional != names:
            raise ValueError("immutable_environments.chunk_store fields do not match schema")
        node, nydusd = raw.get("store_node"), raw.get("nydusd")
        result = cls(**{**raw, "store_node": None if node is None else StoreNodeConfig.from_dict(node),
                        "nydusd": None if nydusd is None else NydusdConfig.from_dict(nydusd)})
        if result.nydusd is not None and result.store_node is None:
            raise ValueError("immutable_environments.chunk_store.nydusd needs store_node")
        if result.nydus_image_sha256 is not None and not (isinstance(result.nydus_image_sha256, str)
                                                          and re.fullmatch("[0-9a-f]{64}", result.nydus_image_sha256)):
            raise ValueError("immutable_environments.chunk_store.nydus_image_sha256 must be a sha256 hex digest")
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
        for name in ("store_node", "nydusd", "nydus_image_sha256"):
            if raw[name] is None:
                del raw[name]  # Older releases reject unknown fields.
        if raw.get("store_node") and not raw["store_node"]["replica"]:
            for name in ("replica", "mirror_seconds"):
                del raw["store_node"][name]
        for name in ("data_device", "native_server_sha256", "package_cache"):
            if raw.get("store_node") and raw["store_node"][name] is None:
                del raw["store_node"][name]
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
    # A deleted sandbox's image stays mounted for the next sandbox of that
    # image; the worker collects idle images, least recently used first, only
    # while attached components exceed this percentage of its block devices.
    device_budget_percent: int = DEFAULT_DEVICE_BUDGET_PERCENT
    # Chunk store M2: the gateway sets each create's environment root (from
    # image-roots.sqlite3, else the annotation). Only once every worker runs
    # 0.9.0 with a chunk store: a dispatched root needs both capabilities.
    dispatch_roots: bool = False
    # Volume-free builds (M2 plan §5.4): the gateway regenerates an
    # OCI-released image a build names from its chunk-store root (nydus-image
    # on the gateway); release-oci may then release build inputs.
    regenerate_bases: bool = False
    # Workers share startup traces through the managed registry (plan C2.7),
    # so a node that never attached a component replays another's trace.
    shared_traces: bool = False
    # Chunk-store (RAFS) images, off by default (C2.13, design §9 M1).
    chunk_store: ChunkStoreConfig | None = None
    # What a build publishes: "erofs" components in the registry, or "rafs":
    # the image converted into the chunk store on the builder (docs/image-recipes.md).
    builder_format: str = "erofs"

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
                     "shared_traces", "regenerate_bases"):
            if not isinstance(getattr(result, name), bool):
                raise ValueError(f"immutable environment {name} must be boolean")
        if type(result.attach_concurrency) is not int or not 1 <= result.attach_concurrency <= 256:
            raise ValueError("immutable environment attach_concurrency must be an integer from 1 to 256")
        if type(result.device_budget_percent) is not int or not 1 <= result.device_budget_percent <= 100:
            raise ValueError("immutable environment device_budget_percent must be an integer from 1 to 100")
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
        if result.regenerate_bases and (result.chunk_store is None or result.chunk_store.store_node is None):
            raise ValueError("immutable environment regenerate_bases reads roots through chunk_store.store_node")
        if result.builder_format not in ("erofs", "rafs"):
            raise ValueError("immutable environment builder_format must be erofs or rafs")
        if result.builder_format == "rafs" and (not result.builder_enabled or result.chunk_store is None
                                                or result.chunk_store.nydus_image_sha256 is None):
            raise ValueError("builder_format rafs needs builder_enabled and a chunk_store with nydus_image_sha256")
        return result

    def to_dict(self):
        # Older releases reject unknown fields: write the switch only once set,
        # so a release rollback still reads configs rendered with it off.
        raw = asdict(self)
        for name in ("preserve_mtimes", "regenerate_bases"):
            if not raw[name]:
                del raw[name]
        if self.builder_format == "erofs":
            del raw["builder_format"]
        if self.device_budget_percent == DEFAULT_DEVICE_BUDGET_PERCENT:
            del raw["device_budget_percent"]
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
