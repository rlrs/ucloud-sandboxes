"""Render optional immutable-image physical I/O bootstrap, independent of agent lifetime."""
import base64
import json
import shlex

from .environment_config import DEFAULT_DEVICE_BUDGET_PERCENT

TRUST_FILE = "/etc/ucloud-sandboxes/environment/producers.json"
KEY_FILE = "/etc/ucloud-sandboxes/environment/producer.pem"
SOCKET = "/run/ucloud-environment/io.sock"
SERVICE = "ucloud-environment-io.service"
CHUNK_TOKEN_FILE = "/etc/ucloud-sandboxes/environment/chunk-index.token"


def settings(options):
    if not options.environment_registry_url:
        return "", "", ""
    registry_flags = (
        " --environment-registry-url " + shlex.quote(options.environment_registry_url)
        + " --environment-registry-repository " + shlex.quote(options.environment_repository)
        + " --environment-trusted-keys " + TRUST_FILE
    )
    trust = base64.b64encode(options.environment_trusted_keys_json.encode()).decode()
    setup = f'''\n$SUDO install -d -m 0755 /etc/ucloud-sandboxes/environment
printf %s {shlex.quote(trust)} | base64 -d | $SUDO tee {TRUST_FILE} >/dev/null
$SUDO chmod 0644 {TRUST_FILE}
'''
    if options.role == "builder":
        private = base64.b64encode(options.environment_signing_key_pem.encode()).decode()
        setup += f'''command -v mkfs.erofs >/dev/null || {{ echo 'immutable builder requires bundled erofs-utils' >&2; exit 1; }}
$SUDO install -m 0600 /dev/null {KEY_FILE}
printf %s {shlex.quote(private)} | base64 -d | $SUDO tee {KEY_FILE} >/dev/null
$SUDO chown root:root {KEY_FILE}
'''
        preserve = " --environment-preserve-mtimes" if options.environment_preserve_mtimes else ""
        if preserve:
            # Layout 2 needs --mkfs-time (1.8+) and --MZ (1.9+). Capture the whole usage: under
            # pipefail, grep -q exiting at the match can SIGPIPE mkfs and fail a capable builder.
            setup += ('UCLOUD_MKFS_USAGE="$(mkfs.erofs --help 2>&1 || true)"; for UCLOUD_MKFS_OPTION in --mkfs-time '
                      '--MZ; do case "$UCLOUD_MKFS_USAGE" in *"$UCLOUD_MKFS_OPTION"*) ;; *) echo "layout-2 publication'
                      ' requires erofs-utils 1.9+ (mkfs.erofs $UCLOUD_MKFS_OPTION)" >&2; exit 1 ;; esac; done\n')
        return (registry_flags + " --environment-signing-key " + KEY_FILE + preserve + "".join(
            " --environment-allow-path " + shlex.quote(path) for path in options.environment_allow_paths), setup, "")
    chunk_setup = chunk_flags = ""
    if options.environment_chunk_index_url:
        # Chunk-store images: the index's read token, never an S3 key.
        token = base64.b64encode(options.environment_chunk_index_token.encode()).decode()
        chunk_setup = (f"$SUDO install -m 0600 /dev/null {CHUNK_TOKEN_FILE}\n"
                       f"printf %s {shlex.quote(token)} | base64 -d | $SUDO tee {CHUNK_TOKEN_FILE} >/dev/null\n")
        chunk_flags = (" --chunk-index-url " + shlex.quote(options.environment_chunk_index_url)
                       + " --chunk-index-token-file " + CHUNK_TOKEN_FILE
                       + f" --chunk-concurrent-misses {int(options.environment_chunk_concurrent_misses)}")
        if options.environment_chunk_store_url:  # C2.6: the store node is the only source.
            chunk_flags += " --chunk-store-url " + shlex.quote(options.environment_chunk_store_url)
        if options.environment_chunk_nydusd:  # C2.1: nydusd serves RAFS images.
            chunk_flags += (" --nydusd " + shlex.quote(options.environment_chunk_nydusd)
                            + " --nydusd-sha256 " + options.environment_chunk_nydusd_sha256)
    setup += f'''# Never replace the adapter beneath existing sandboxes.
if [ -e "$UCLOUD_STATE_DIR/direct-runtime/direct-registry.sqlite" ] && [ ! -e "$UCLOUD_STATE_DIR/environment-adapter" ]; then
  echo 'immutable environments require a fresh worker; retire the old adapter first' >&2; exit 1
fi
$SUDO modprobe erofs
# Do not change a live nodewide device pool. Dedicated fresh-worker pool only.
# One device serves each distinct mounted component, so the pool bounds the
# distinct components a worker mounts at once: per-layer images share their
# base components but add one or more of their own (hundreds at target density).
if [ ! -d /sys/module/nbd ]; then $SUDO modprobe nbd nbds_max=1024 max_part=0; fi
test -b /dev/nbd0
$SUDO touch "$UCLOUD_STATE_DIR/environment-adapter"
{chunk_setup}$SUDO tee /etc/systemd/system/{SERVICE} >/dev/null <<ENVIRONMENT_IO_SERVICE
[Unit]
Description=UCloud verified immutable environment I/O
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=root
Group=root
PrivateMounts=no
RuntimeDirectory=ucloud-environment
RuntimeDirectoryMode=0700
ExecStart=$UCLOUD_AGENT_BIN serve-environment-io --root $UCLOUD_STATE_DIR/environment-io --socket {SOCKET} --cache-bytes {options.environment_cache_bytes}{"" if options.environment_prefetch_enabled else " --disable-prefetch"}{"" if options.environment_attach_concurrency == 1 else f" --attach-concurrency {int(options.environment_attach_concurrency)}"}{" --shared-traces" if options.environment_shared_traces else ""}{chunk_flags}{registry_flags}
Restart=no
# Every attached image holds NBD connections, sockets and, for RAFS, its
# bootstrap: a 512-rollout burst hit the default 1024 at about 140 images.
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
ENVIRONMENT_IO_SERVICE
'''
    # Start (never restart): active filesystem devices outlive frontend upgrades.
    start = f"$SUDO systemctl enable {SERVICE}\n$SUDO systemctl start {SERVICE}\n"
    rafs = " --environment-rafs" if options.environment_chunk_store_url else ""
    budget = options.environment_device_budget_percent
    # Absent at the default, so the rendered init is unchanged.
    rafs += "" if budget == DEFAULT_DEVICE_BUDGET_PERCENT else f" --environment-device-budget-percent {int(budget)}"
    return registry_flags + " --environment-backend-socket " + SOCKET + rafs, setup, start


def validate(options):
    supplied = bool(options.environment_registry_url)
    if not supplied:
        if any((options.environment_repository, options.environment_trusted_keys_json,
                options.environment_signing_key_pem, options.environment_allow_paths,
                options.environment_preserve_mtimes, options.environment_chunk_index_url,
                options.environment_chunk_store_url, options.environment_chunk_nydusd)):
            raise ValueError("immutable environment bootstrap requires registry URL and producer trust")
        return
    from .environment_config import EnvironmentDeploymentConfig
    EnvironmentDeploymentConfig.from_dict({
        "trusted_keys_file": TRUST_FILE, "signing_key_file": KEY_FILE if options.role == "builder" else "",
        "repository": options.environment_repository, "worker_enabled": options.role == "sandbox",
        "builder_enabled": options.role == "builder", "allow_paths": options.environment_allow_paths,
        "cache_bytes": options.environment_cache_bytes, "preserve_mtimes": options.environment_preserve_mtimes,
        "prefetch_enabled": options.environment_prefetch_enabled,
        "shared_traces": options.environment_shared_traces,
        "device_budget_percent": options.environment_device_budget_percent,
    })
    from .environment_artifact import content_digest
    raw = json.loads(options.environment_trusted_keys_json)
    if not isinstance(raw, dict) or not raw or len(options.environment_trusted_keys_json) > 65536:
        raise ValueError("invalid immutable environment producer trust")
    keys = {key: base64.b64decode(value, validate=True) for key, value in raw.items()}
    if any(len(value) != 32 or content_digest(value) != key for key, value in keys.items()):
        raise ValueError("invalid immutable environment producer key identity")
    if not options.environment_registry_url.startswith(("http://", "https://")) or any(c in options.environment_registry_url for c in "\0\r\n"):
        raise ValueError("invalid immutable environment registry URL")
    chunk_url, chunk_token = options.environment_chunk_index_url, options.environment_chunk_index_token
    if (bool(chunk_url) != bool(chunk_token) or (chunk_url and options.role != "sandbox")
            or (chunk_url and (not chunk_url.startswith(("http://", "https://")) or not 32 <= len(chunk_token) <= 4096
                               or any(c in chunk_url + chunk_token for c in "\0\r\n '\"")))):
        raise ValueError("invalid immutable environment chunk index bootstrap")
    store_url = options.environment_chunk_store_url
    if store_url and (not chunk_url or not store_url.startswith(("http://", "https://"))
                      or any(c in store_url for c in "\0\r\n '\"")):
        raise ValueError("invalid immutable environment chunk store node bootstrap")
    if options.environment_chunk_nydusd or options.environment_chunk_nydusd_sha256:
        from .environment_config import NydusdConfig
        NydusdConfig.from_dict({"path": options.environment_chunk_nydusd,
                                "sha256": options.environment_chunk_nydusd_sha256})
        if not store_url:
            raise ValueError("nydusd needs the chunk store node")
    if options.role == "sandbox":
        if options.environment_signing_key_pem:
            raise ValueError("sandbox workers must never receive environment signing keys")
        if options.environment_preserve_mtimes:
            raise ValueError("only builders publish environment components")
        if options.environment_cache_bytes > options.direct_disk_headroom_mb * 1024 ** 2 // 2:
            raise ValueError("immutable environment cache must leave half the disk safety headroom free")
    else:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PublicFormat
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        if len(options.environment_signing_key_pem) > 16384:
            raise ValueError("environment signing key too large")
        key = load_pem_private_key(options.environment_signing_key_pem.encode(), password=None)
        if not isinstance(key, Ed25519PrivateKey):
            raise ValueError("environment signing key must be Ed25519")
        public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        if keys.get(content_digest(public)) != public:
            raise ValueError("environment signing key is not trusted")
