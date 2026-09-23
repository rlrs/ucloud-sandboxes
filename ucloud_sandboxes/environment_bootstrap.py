"""Render optional immutable-image physical I/O bootstrap, independent of agent lifetime."""
import base64
import json
import shlex


TRUST_FILE = "/etc/ucloud-sandboxes/environment/producers.json"
KEY_FILE = "/etc/ucloud-sandboxes/environment/producer.pem"
SOCKET = "/run/ucloud-environment/io.sock"
SERVICE = "ucloud-environment-io.service"


def settings(options):
    enabled = bool(options.environment_registry_url)
    if not enabled:
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
        return (registry_flags + " --environment-signing-key " + KEY_FILE + "".join(
            " --environment-allow-path " + shlex.quote(path) for path in options.environment_allow_paths), setup, "")
    setup += f'''# Never replace the adapter beneath existing sandboxes.
if [ -e "$UCLOUD_STATE_DIR/direct-runtime/direct-registry.sqlite" ] && [ ! -e "$UCLOUD_STATE_DIR/environment-adapter" ]; then
  echo 'immutable environments require a fresh worker; retire the old adapter first' >&2; exit 1
fi
$SUDO modprobe erofs
# Do not change a live nodewide device pool. Dedicated fresh-worker pool only.
if [ ! -d /sys/module/nbd ]; then $SUDO modprobe nbd nbds_max=64 max_part=0; fi
test -b /dev/nbd0
$SUDO touch "$UCLOUD_STATE_DIR/environment-adapter"
$SUDO tee /etc/systemd/system/{SERVICE} >/dev/null <<ENVIRONMENT_IO_SERVICE
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
ExecStart=$UCLOUD_AGENT_BIN serve-environment-io --root $UCLOUD_STATE_DIR/environment-io --socket {SOCKET} --cache-bytes {options.environment_cache_bytes}{registry_flags}
Restart=no

[Install]
WantedBy=multi-user.target
ENVIRONMENT_IO_SERVICE
'''
    # Start (never restart): active filesystem devices outlive frontend upgrades.
    start = f"$SUDO systemctl enable {SERVICE}\n$SUDO systemctl start {SERVICE}\n"
    return registry_flags + " --environment-backend-socket " + SOCKET, setup, start


def validate(options):
    supplied = bool(options.environment_registry_url)
    if not supplied:
        if any((options.environment_repository, options.environment_trusted_keys_json,
                options.environment_signing_key_pem, options.environment_allow_paths)):
            raise ValueError("immutable environment bootstrap requires registry URL and producer trust")
        return
    from .environment_config import EnvironmentDeploymentConfig
    EnvironmentDeploymentConfig.from_dict({
        "trusted_keys_file": TRUST_FILE, "signing_key_file": KEY_FILE if options.role == "builder" else "",
        "repository": options.environment_repository, "worker_enabled": options.role == "sandbox",
        "builder_enabled": options.role == "builder", "allow_paths": options.environment_allow_paths,
        "cache_bytes": options.environment_cache_bytes,
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
    if options.role == "sandbox":
        if options.environment_signing_key_pem:
            raise ValueError("sandbox workers must never receive environment signing keys")
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
