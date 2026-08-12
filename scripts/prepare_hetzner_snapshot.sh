#!/usr/bin/env bash
set -euo pipefail

# Prepare a deliberately disposable Hetzner sandbox-node source VM for imaging.
# The hostname guard prevents this cleanup from being run on an arbitrary node.
expected_hostname="${1:-}"
expected_docker_image_gib="${2:-}"
if [[ -z "$expected_hostname" || "$(hostname)" != "$expected_hostname" \
  || ! "$expected_docker_image_gib" =~ ^[1-9][0-9]*$ ]]; then
  echo "usage: $0 \$(hostname) <expected-docker-image-gib>" >&2
  echo "refusing to sanitize unexpected host: $(hostname)" >&2
  exit 2
fi
if [[ "${EUID}" -ne 0 ]]; then
  echo "snapshot preparation must run as root" >&2
  exit 2
fi
if [[ ! -f /var/lib/ucloud-sandboxes/docker-xfs.img ]]; then
  echo "sandbox-node XFS image is missing" >&2
  exit 2
fi
expected_docker_image_bytes=$((expected_docker_image_gib * 1024 * 1024 * 1024))
actual_docker_image_bytes="$(stat -c '%s' /var/lib/ucloud-sandboxes/docker-xfs.img)"
if [[ "$actual_docker_image_bytes" != "$expected_docker_image_bytes" ]]; then
  echo "Docker XFS image has unexpected size: $actual_docker_image_bytes bytes" >&2
  exit 2
fi
if [[ ! -L /lib || "$(readlink /lib)" != "usr/lib" ]]; then
  echo "merged-/usr invariant is broken: /lib must point to usr/lib" >&2
  exit 2
fi
runtime_cache_root=/var/cache/ucloud-sandboxes/init-packages
shopt -s nullglob
runtime_receipts=("$runtime_cache_root"/*/runtime-ready-v*-*)
shopt -u nullglob
if [[ "${#runtime_receipts[@]}" -ne 1 ]]; then
  echo "exactly one snapshot-ready runtime receipt is required" >&2
  exit 2
fi
runtime_receipt="${runtime_receipts[0]}"
runtime_cache_dir="$(dirname "$runtime_receipt")"
runtime_bundle="$runtime_cache_dir/node-package.tar.gz"
runtime_marker="$runtime_bundle.sha256"
if [[ "$(stat -c '%u:%a' "$runtime_receipt")" != "0:444" \
  || "$(stat -c '%u:%a' "$runtime_bundle")" != "0:444" \
  || ! -f "$runtime_marker" ]]; then
  echo "snapshot runtime cache must be immutable and root-owned" >&2
  exit 2
fi
runtime_sha256="$(awk -F= '$1 == "bundle_sha256" {print $2; exit}' "$runtime_receipt")"
if [[ ! "$runtime_sha256" =~ ^[0-9a-f]{64}$ \
  || "$(cat "$runtime_marker")" != "$runtime_sha256" \
  || "$(sha256sum "$runtime_bundle" | awk '{print $1}')" != "$runtime_sha256" \
  || "$(awk -F= '$1 == "kernel_release" {print $2; exit}' "$runtime_receipt")" != "$(uname -r)" \
  || ! -d "$runtime_cache_dir/agent-runtime/site-packages/ucloud_sandboxes" ]]; then
  echo "snapshot runtime receipt does not match its cached runtime" >&2
  exit 2
fi
for boot_module in nls_iso8859_1 xfs overlay ublk_drv; do
  if ! modprobe "$boot_module"; then
    echo "required boot/runtime module is unavailable: $boot_module" >&2
    exit 2
  fi
done

echo "Stopping workload and test services"
systemctl disable --now ucloud-sandbox-heartbeat.timer 2>/dev/null || true
systemctl stop \
  ucloud-sandbox-node.service \
  ucloud-storage-native.service \
  ucloud-storage-native-backend.service \
  ucloud-sandbox-heartbeat.service 2>/dev/null || true
if [[ -f /tmp/ucloud-hetzner-heartbeat.pid ]]; then
  kill "$(cat /tmp/ucloud-hetzner-heartbeat.pid)" 2>/dev/null || true
fi
docker rm -f ucloud-snapshot-test-registry 2>/dev/null || true
systemctl stop docker.service docker.socket containerd.service 2>/dev/null || true

echo "Resetting reusable local storage"
swapoff /var/lib/ucloud-sandboxes/swapfile 2>/dev/null || true
sed -i '\|^/var/lib/ucloud-sandboxes/swapfile none swap sw 0 0$|d' /etc/fstab
rm -f /var/lib/ucloud-sandboxes/swapfile
umount /var/lib/ucloud-sandboxes/docker-xfs 2>/dev/null || true
if findmnt -M /var/lib/ucloud-sandboxes/docker-xfs >/dev/null 2>&1; then
  echo "Docker XFS data root is still mounted" >&2
  exit 1
fi
mkfs.xfs -f -m reflink=1 /var/lib/ucloud-sandboxes/docker-xfs.img >/dev/null

echo "Removing source-node state and build inputs"
rm -rf "$runtime_cache_dir/extracted"
find "$runtime_cache_root" -mindepth 1 -maxdepth 1 -type d \
  ! -path "$runtime_cache_dir" -exec rm -rf -- {} +
rm -rf \
  /work/ucloud-sandboxes/state \
  /var/lib/ucloud-sandboxes/storage-native \
  /var/lib/ucloud-sandboxes/storage-native-cache \
  /var/lib/docker \
  /var/lib/containerd \
  /root/.cache \
  /root/.cargo \
  /root/.docker \
  /root/.rustup \
  /root/AgentENV \
  /root/node-bundle-input \
  /root/node-bundle-output \
  /root/node-bundle-venv \
  /root/ucloud-repack-node-agent.py \
  /root/ucloud_sandboxes-*.whl \
  /root/sandbox-node-package*.tar.gz \
  /root/sandbox-node-package*.tar.gz.sha256 \
  /root/storage-artifacts \
  /root/storage_native
install -d -m 0700 -o root -g root /work/ucloud-sandboxes/state
install -d -m 0700 -o root -g root \
  /var/lib/ucloud-sandboxes/storage-native \
  /var/lib/ucloud-sandboxes/storage-native/runtime \
  /var/lib/ucloud-sandboxes/storage-native/mounts

echo "Removing source-node configuration and credentials"
rm -f \
  /etc/ucloud-sandboxes/heartbeat-token \
  /etc/ucloud-sandboxes/node-control-token \
  /etc/ucloud-sandboxes/node.env \
  /etc/ucloud-sandboxes/storage-native-backend.json \
  /etc/ucloud-sandboxes/storage-native-resize-backend.json \
  /etc/systemd/system/ucloud-sandbox-heartbeat.service \
  /etc/systemd/system/ucloud-sandbox-heartbeat.timer \
  /etc/systemd/system/ucloud-sandbox-node.service \
  /etc/systemd/system/ucloud-storage-native.service \
  /etc/systemd/system/ucloud-storage-native-backend.service \
  /tmp/ucloud-hetzner-heartbeat.py \
  /tmp/ucloud-hetzner-heartbeat.pid \
  /tmp/ucloud-snapshot-smoke.py \
  /root/qualify_hetzner_host.sh \
  /root/prepare_hetzner_snapshot.sh \
  /root/.bash_history
rm -rf \
  /tmp/ucloud-snapshot-test-registry \
  /etc/systemd/system/multi-user.target.wants/ucloud-* \
  /etc/systemd/system/timers.target.wants/ucloud-* \
  /run/ucloud-sandboxes
systemctl daemon-reload

echo "Cleaning package caches, logs, leases, and machine identity"
apt-get clean
rm -rf /var/lib/apt/lists/* /var/log/ucloud-sandboxes
rm -f /var/lib/systemd/random-seed /var/lib/dhcp/*.lease /var/lib/dhcp/*.leases
journalctl --rotate >/dev/null 2>&1 || true
journalctl --vacuum-time=1s >/dev/null 2>&1 || true
hostnamectl set-hostname localhost
sed -i '/^127\.0\.1\.1[[:space:]]/d' /etc/hosts
install -d -m 0755 /etc/systemd/system/systemd-networkd-wait-online.service.d
network_wait_override=/etc/systemd/system/systemd-networkd-wait-online.service.d/90-ucloud-private-network.conf
install -m 0644 /dev/null "$network_wait_override"
printf '%s\n' \
  '[Service]' \
  'ExecStart=' \
  'ExecStart=/lib/systemd/systemd-networkd-wait-online --any --operational-state=degraded --timeout=10' \
  >"$network_wait_override"
# cloud-init rewrites this from clone metadata. Keeping the source VM's file
# can leave a clone waiting for interfaces that do not exist on its shape.
rm -f /etc/netplan/50-cloud-init.yaml
cloud-init clean --logs --machine-id --configs ssh_config --seed
rm -f /etc/ssh/ssh_host_*

echo "Snapshot source is sanitized; power it off before creating the image."
