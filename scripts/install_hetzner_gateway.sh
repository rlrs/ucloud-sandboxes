#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: install_hetzner_gateway.sh --public-ip <ipv4> [--volume-device <path>]

--volume-device is required only when registry_store.kind=filesystem.

The following release inputs must already be staged on the gateway:
  /tmp/ucloud_sandboxes-0.4.1-py3-none-any.whl
  /tmp/ucloud-sandboxes-deployment.json
  /tmp/ucloud-sandboxes-node-package.tar.gz
  /tmp/ucloud-sandboxes-gateway-init
  /tmp/ucloud-sandboxes-gateway-init.pub
  /tmp/ucloud-sandboxes-hetzner.env
  /tmp/configure_hetzner_sdk_ingress.sh
EOF
}

public_ip=""
volume_device=""
while (($#)); do
  case "$1" in
    --public-ip)
      if (($# < 2)); then usage; exit 2; fi
      public_ip="$2"
      shift 2
      ;;
    --volume-device)
      if (($# < 2)); then usage; exit 2; fi
      volume_device="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

if [[ "$EUID" -ne 0 ]]; then
  echo "Hetzner gateway installation must run as root" >&2
  exit 2
fi
if [[ ! "$public_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "--public-ip must be an IPv4 address" >&2
  exit 2
fi
wheel=/tmp/ucloud_sandboxes-0.4.1-py3-none-any.whl
deployment=/tmp/ucloud-sandboxes-deployment.json
node_bundle=/tmp/ucloud-sandboxes-node-package.tar.gz
init_key=/tmp/ucloud-sandboxes-gateway-init
init_public_key=/tmp/ucloud-sandboxes-gateway-init.pub
provider_env=/tmp/ucloud-sandboxes-hetzner.env
ingress=/tmp/configure_hetzner_sdk_ingress.sh
for required in \
  "$wheel" \
  "$deployment" \
  "$node_bundle" \
  "$init_key" \
  "$init_public_key" \
  "$provider_env" \
  "$ingress"; do
  if [[ ! -s "$required" ]]; then
    echo "missing staged gateway input: $required" >&2
    exit 2
  fi
done

registry_store_kind="$(python3 - "$deployment" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    payload = json.load(source)
store = payload.get("registry_store")
if isinstance(store, dict):
    print(store.get("kind", ""))
else:
    # Schema 1-3 deployments used the filesystem registry implicitly.
    print("filesystem")
PY
)"
if [[ "$registry_store_kind" != filesystem && "$registry_store_kind" != s3 ]]; then
  echo "deployment registry_store.kind must be filesystem or s3" >&2
  exit 2
fi
if [[ "$registry_store_kind" == filesystem && \
      ( -z "$volume_device" || "$volume_device" != /dev/* ) ]]; then
  echo "--volume-device must be an absolute /dev path for filesystem registry storage" >&2
  exit 2
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  ca-certificates curl docker.io nftables openssl python3-venv
systemctl enable --now docker.service

registry_mount=/mnt/ucloud-registry
if [[ "$registry_store_kind" == filesystem ]]; then
  for attempt in $(seq 1 60); do
    if [[ -b "$volume_device" ]]; then break; fi
    if [[ "$attempt" == 60 ]]; then
      echo "registry Volume did not appear: $volume_device" >&2
      exit 1
    fi
    sleep 1
  done
  if [[ "$(blkid -o value -s TYPE "$volume_device")" != ext4 ]]; then
    echo "registry Volume is not the expected preformatted ext4 filesystem" >&2
    exit 1
  fi
  install -d -m 0755 "$registry_mount"
  volume_uuid="$(blkid -o value -s UUID "$volume_device")"
  if ! grep -qF "UUID=$volume_uuid $registry_mount " /etc/fstab; then
    printf 'UUID=%s %s ext4 defaults,nofail,x-systemd.device-timeout=30s 0 2\n' \
      "$volume_uuid" "$registry_mount" >>/etc/fstab
  fi
  mount "$registry_mount" 2>/dev/null || mountpoint -q "$registry_mount"
  mountpoint -q "$registry_mount"
  install -d -m 0755 "$registry_mount/docker-registry"
fi

if ! id ucloud >/dev/null 2>&1; then
  useradd --system --create-home \
    --home-dir /var/lib/ucloud-sandboxes \
    --shell /usr/sbin/nologin \
    ucloud
fi

install_root=/work/ucloud-sandboxes
release_dir="$install_root/release"
venv_dir="$install_root/gateway-venv"
data_root=/var/lib/ucloud-sandboxes/state
install -d -m 0755 "$install_root" "$release_dir" /etc/ucloud-sandboxes
install -d -m 0700 -o ucloud -g ucloud "$data_root" "$data_root/ssh"
install -m 0644 "$node_bundle" "$release_dir/sandbox-node-package.tar.gz"
install -m 0644 "$node_bundle" "$release_dir/builder-node-package.tar.gz"
install -m 0644 "$deployment" /etc/ucloud-sandboxes/deployment.json
install -m 0600 "$provider_env" /etc/ucloud-sandboxes/hetzner.env
install -m 0600 "$provider_env" /etc/ucloud-sandboxes/snapshot-store.env
install -m 0600 "$provider_env" /etc/ucloud-sandboxes/registry-store.env
install -m 0600 -o ucloud -g ucloud "$init_key" "$data_root/ssh/gateway-init"
install -m 0644 -o ucloud -g ucloud \
  "$init_public_key" "$data_root/ssh/gateway-init.pub"

python3 -m venv "$venv_dir"
"$venv_dir/bin/pip" install --disable-pip-version-check --force-reinstall "$wheel"

for name in \
  gateway-token \
  sandbox-api-token \
  heartbeat-token \
  node-control-token \
  relay-sandbox-token \
  relay-worker-token; do
  token_path="$data_root/$name"
  if [[ ! -s "$token_path" ]]; then
    umask 077
    openssl rand -hex 32 >"$token_path"
  fi
  chown ucloud:ucloud "$token_path"
  chmod 0600 "$token_path"
done

systemd_source="$($venv_dir/bin/python - <<'PY'
from importlib import resources
print(resources.files("ucloud_sandboxes").joinpath("systemd"))
PY
)"
for unit in \
  ucloud-sandbox-autoscaler.service \
  ucloud-sandbox-gateway.service \
  ucloud-sandbox-registry-gc.service \
  ucloud-sandbox-registry-gc.timer \
  ucloud-sandbox-snapshot-gc.service \
  ucloud-sandbox-snapshot-gc.timer \
  ucloud-sandbox-registry-prune.service \
  ucloud-sandbox-registry-prune.timer \
  ucloud-sandbox-registry.service \
  ucloud-sandbox-relay.service; do
  install -m 0644 "$systemd_source/$unit" "/etc/systemd/system/$unit"
done

install -d -m 0755 \
  /etc/systemd/system/ucloud-sandbox-autoscaler.service.d \
  /etc/systemd/system/ucloud-sandbox-registry.service.d \
  /etc/systemd/system/ucloud-sandbox-registry-gc.service.d
cat >/etc/systemd/system/ucloud-sandbox-autoscaler.service.d/hetzner.conf <<'EOF'
[Service]
EnvironmentFile=/etc/ucloud-sandboxes/hetzner.env
EOF
for unit in \
  ucloud-sandbox-registry.service \
  ucloud-sandbox-registry-gc.service; do
  if [[ "$registry_store_kind" == filesystem ]]; then
    cat >"/etc/systemd/system/$unit.d/volume.conf" <<'EOF'
[Unit]
RequiresMountsFor=/mnt/ucloud-registry

[Service]
ExecStartPre=/usr/bin/mountpoint -q /mnt/ucloud-registry
EOF
  else
    rm -f "/etc/systemd/system/$unit.d/volume.conf"
  fi
done

cat >/usr/local/sbin/ucloud-sandboxes-nat <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
public_if="$(ip -4 route show default | awk 'NR == 1 {print $5}')"
private_if="$(ip -o -4 address show | awk '$4 ~ /^10\.42\.0\.2\// {print $2; exit}')"
if [[ -z "$public_if" || -z "$private_if" ]]; then
  echo "could not resolve Hetzner public/private interfaces" >&2
  exit 1
fi
nft delete table inet ucloud_sandboxes_nat 2>/dev/null || true
nft -f - <<NFT
table inet ucloud_sandboxes_nat {
  chain postrouting {
    type nat hook postrouting priority srcnat; policy accept;
    oifname "$public_if" ip saddr 10.42.0.0/24 masquerade
  }
}
NFT

# Docker owns the iptables-nft FORWARD base chain and gives it a DROP policy.
# An ACCEPT verdict in a separate nftables base chain is not final, so private
# worker forwarding must also be allowed through Docker's documented user
# chain. Keep our rules in a dedicated chain so reruns replace them atomically.
iptables -N UCLOUD-SANDBOXES 2>/dev/null || true
iptables -F UCLOUD-SANDBOXES
iptables -A UCLOUD-SANDBOXES \
  -i "$private_if" -o "$public_if" -s 10.42.0.0/24 -j ACCEPT
iptables -A UCLOUD-SANDBOXES \
  -i "$public_if" -o "$private_if" -d 10.42.0.0/24 \
  -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
iptables -A UCLOUD-SANDBOXES -j RETURN
if ! iptables -C DOCKER-USER -j UCLOUD-SANDBOXES 2>/dev/null; then
  iptables -I DOCKER-USER 1 -j UCLOUD-SANDBOXES
fi
EOF
chmod 0755 /usr/local/sbin/ucloud-sandboxes-nat
cat >/etc/sysctl.d/90-ucloud-sandboxes-gateway.conf <<'EOF'
net.ipv4.ip_forward=1
EOF
cat >/etc/systemd/system/ucloud-sandboxes-nat.service <<'EOF'
[Unit]
Description=UCloud Sandboxes private worker NAT
After=docker.service network-online.target
Wants=docker.service network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/ucloud-sandboxes-nat
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
sysctl --system >/dev/null

docker pull registry:2
systemctl daemon-reload
systemctl enable --now ucloud-sandboxes-nat.service
systemctl enable ucloud-sandbox-registry.service
systemctl enable ucloud-sandbox-gateway.service
systemctl enable ucloud-sandbox-relay.service
systemctl enable ucloud-sandbox-autoscaler.service
systemctl enable --now \
  ucloud-sandbox-registry-prune.timer \
  ucloud-sandbox-registry-gc.timer \
  ucloud-sandbox-snapshot-gc.timer
systemctl restart ucloud-sandbox-registry.service

for url in http://127.0.0.1:5000/v2/; do
  for attempt in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null; then break; fi
    if [[ "$attempt" == 60 ]]; then
      systemctl --no-pager --full status ucloud-sandbox-registry.service
      exit 1
    fi
    sleep 1
  done
done

systemctl restart ucloud-sandbox-gateway.service
systemctl restart ucloud-sandbox-relay.service
systemctl restart ucloud-sandbox-autoscaler.service
for url in http://127.0.0.1:8090/healthz http://127.0.0.1:8092/healthz; do
  for attempt in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null; then break; fi
    if [[ "$attempt" == 60 ]]; then
      systemctl --no-pager --full status \
        ucloud-sandbox-gateway.service \
        ucloud-sandbox-relay.service \
        ucloud-sandbox-autoscaler.service
      exit 1
    fi
    sleep 1
  done
done

install -m 0755 "$ingress" /usr/local/sbin/configure-hetzner-sdk-ingress
/usr/local/sbin/configure-hetzner-sdk-ingress --public-host "$public_ip"

rm -f \
  "$wheel" \
  "$deployment" \
  "$node_bundle" \
  "$init_key" \
  "$init_public_key" \
  "$provider_env" \
  "$ingress"

systemctl --no-pager --full status \
  ucloud-sandbox-registry.service \
  ucloud-sandbox-gateway.service \
  ucloud-sandbox-relay.service \
  ucloud-sandbox-autoscaler.service \
  ucloud-sandboxes-nat.service
printf 'sdk_url=https://%s\n' "$public_ip"
printf 'sdk_token_file=%s\n' "$data_root/sandbox-api-token"
