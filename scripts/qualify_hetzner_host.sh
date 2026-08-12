#!/usr/bin/env bash
set -uo pipefail

# Destructive host qualification for a disposable Hetzner Cloud VM.
# It installs runtime packages and creates temporary loop, namespace, Docker,
# and gVisor state. Never run it on a machine containing valuable data.

export DEBIAN_FRONTEND=noninteractive
failures=0

pass() {
  printf 'PASS\t%s\t%s\n' "$1" "${2:-ok}"
}

fail() {
  printf 'FAIL\t%s\t%s\n' "$1" "${2:-failed}"
  failures=$((failures + 1))
}

warn() {
  printf 'WARN\t%s\t%s\n' "$1" "${2:-warning}"
}

section() {
  printf 'SECTION\t%s\n' "$1"
}

probe_root="$(mktemp -d /var/tmp/ucloud-hetzner-probe.XXXXXX)"
xfs_image="$probe_root/xfs.img"
xfs_mount="$probe_root/xfs"
xfs_loop=""
netns="ucloud-probe-$$"

cleanup() {
  docker rm -f ucloud-dind-probe >/dev/null 2>&1 || true
  docker rm -f ucloud-port-probe >/dev/null 2>&1 || true
  docker image rm -f ucloud-build-probe >/dev/null 2>&1 || true
  nft delete table inet ucloud_probe >/dev/null 2>&1 || true
  ip netns delete "$netns" >/dev/null 2>&1 || true
  mountpoint -q "$probe_root/overlay/merged" && umount "$probe_root/overlay/merged"
  mountpoint -q "$xfs_mount" && umount "$xfs_mount"
  if [[ -n "$xfs_loop" ]]; then
    losetup -d "$xfs_loop" >/dev/null 2>&1 || true
  fi
  rm -rf "$probe_root"
}
trap cleanup EXIT

section host
printf 'INFO\tos_release\t%s\n' "$(. /etc/os-release && printf '%s %s' "$ID" "$VERSION_ID")"
printf 'INFO\tkernel\t%s\n' "$(uname -r)"
printf 'INFO\tarchitecture\t%s\n' "$(uname -m)"
printf 'INFO\tvirtualization\t%s\n' "$(systemd-detect-virt 2>/dev/null || true)"
printf 'INFO\tcpu_count\t%s\n' "$(nproc)"
printf 'INFO\tmemory_mib\t%s\n' "$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1024 ))"
printf 'INFO\troot_disk\t%s\n' "$(df -h --output=size,avail / | tail -n 1 | xargs)"

if [[ "$(. /etc/os-release && printf '%s' "$ID")" == ubuntu ]]; then
  pass ubuntu
else
  fail ubuntu "expected Ubuntu"
fi

section packages
base_packages=(
  acl bzip2 ca-certificates curl fio gnupg iproute2 iputils-ping jq nftables
  uidmap xfsprogs
)
if apt-get update -qq; then
  modules_extra="linux-modules-extra-$(uname -r)"
  if apt-cache show "$modules_extra" >/dev/null 2>&1; then
    base_packages+=("$modules_extra")
  fi
fi
if apt-get install -y -qq "${base_packages[@]}" \
    >/var/tmp/ucloud-probe-apt.log 2>&1; then
  pass base_packages
else
  fail base_packages "see /var/tmp/ucloud-probe-apt.log"
fi

install -m 0755 -d /etc/apt/keyrings
if curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    -o /etc/apt/keyrings/docker.asc \
  && chmod a+r /etc/apt/keyrings/docker.asc; then
  docker_arch="$(dpkg --print-architecture)"
  docker_codename="$(. /etc/os-release && printf '%s' "${UBUNTU_CODENAME:-$VERSION_CODENAME}")"
  rm -f /etc/apt/sources.list.d/docker.list
  cat >/etc/apt/sources.list.d/docker.sources <<DOCKER_SOURCES
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $docker_codename
Components: stable
Architectures: $docker_arch
Signed-By: /etc/apt/keyrings/docker.asc
DOCKER_SOURCES
  if apt-get update -qq && apt-get install -y -qq \
      docker-ce docker-ce-cli containerd.io docker-buildx-plugin \
      >/var/tmp/ucloud-probe-docker-apt.log 2>&1; then
    pass docker_packages
  else
    fail docker_packages "see /var/tmp/ucloud-probe-docker-apt.log"
  fi
else
  fail docker_repository
fi

section kernel_modules
modules=(
  virtiofs xfs overlay ublk_drv bridge br_netfilter veth nf_tables
  nft_chain_nat nft_compat ip_tables iptable_nat xt_addrtype xt_conntrack
  xt_MASQUERADE
)
for module in "${modules[@]}"; do
  if modprobe "$module" >/dev/null 2>&1; then
    pass "module_$module"
  else
    fail "module_$module"
  fi
done

if [[ -e /dev/ublk-control || -e /dev/ublkc0 || -d /sys/class/ublk-char ]]; then
  pass ublk_control_device
else
  fail ublk_control_device "ublk_drv loaded without a visible control device"
fi

section isolation_primitives
if [[ "$(stat -fc %T /sys/fs/cgroup)" == cgroup2fs ]]; then
  pass cgroup_v2
else
  fail cgroup_v2
fi

if systemd-run --quiet --wait --collect \
    -p MemoryMax=64M -p CPUQuota=50% -p TasksMax=16 /bin/true; then
  pass cgroup_limits
else
  fail cgroup_limits
fi

if unshare --user --map-root-user --mount --pid --fork /bin/true; then
  pass user_mount_pid_namespaces
else
  fail user_mount_pid_namespaces
fi

section filesystems
mkdir -p "$probe_root/overlay"/{lower,upper,work,merged}
printf 'overlay-ok\n' >"$probe_root/overlay/lower/value"
if mount -t overlay overlay \
    -o "lowerdir=$probe_root/overlay/lower,upperdir=$probe_root/overlay/upper,workdir=$probe_root/overlay/work" \
    "$probe_root/overlay/merged" \
  && grep -qx overlay-ok "$probe_root/overlay/merged/value"; then
  pass overlay_mount
else
  fail overlay_mount
fi

mkdir -p "$xfs_mount"
truncate -s 768M "$xfs_image"
xfs_loop="$(losetup --find --show "$xfs_image")"
if mkfs.xfs -f -m reflink=1 "$xfs_loop" >/var/tmp/ucloud-probe-mkfs.log 2>&1 \
  && mount -o pquota "$xfs_loop" "$xfs_mount"; then
  pass xfs_mount_pquota
else
  fail xfs_mount_pquota "see /var/tmp/ucloud-probe-mkfs.log"
fi

if xfs_info "$xfs_mount" 2>/dev/null | grep -q 'reflink=1'; then
  pass xfs_reflink_enabled
else
  fail xfs_reflink_enabled
fi

if fallocate -l 32M "$xfs_mount/source" \
  && cp --reflink=always "$xfs_mount/source" "$xfs_mount/clone"; then
  pass xfs_reflink_copy
else
  fail xfs_reflink_copy
fi

mkdir -p "$xfs_mount/project"
if xfs_quota -x -c "project -s -p $xfs_mount/project 42" "$xfs_mount" \
  && xfs_quota -x -c 'limit -p bhard=32m 42' "$xfs_mount"; then
  if dd if=/dev/zero of="$xfs_mount/project/quota-test" bs=1M count=48 status=none \
      2>/var/tmp/ucloud-probe-quota.log; then
    fail xfs_project_quota "write exceeded the hard limit"
  else
    pass xfs_project_quota
  fi
else
  fail xfs_project_quota "could not configure project 42"
fi

section networking
if nft add table inet ucloud_probe \
  && nft 'add chain inet ucloud_probe output { type filter hook output priority 10; policy accept; }' \
  && nft list table inet ucloud_probe >/dev/null; then
  pass nftables_mutation
else
  fail nftables_mutation
fi
nft delete table inet ucloud_probe >/dev/null 2>&1 || true

if ip netns add "$netns" \
  && ip link add uvh0 type veth peer name uvn0 \
  && ip link set uvn0 netns "$netns" \
  && ip address add 192.0.2.1/30 dev uvh0 \
  && ip link set uvh0 up \
  && ip -n "$netns" address add 192.0.2.2/30 dev uvn0 \
  && ip -n "$netns" link set lo up \
  && ip -n "$netns" link set uvn0 up \
  && ip netns exec "$netns" ping -c 2 -W 2 192.0.2.1 >/dev/null; then
  pass network_namespace_veth
else
  fail network_namespace_veth
fi

section docker
install -m 0755 -d /etc/docker
printf '%s\n' \
  '{"storage-driver":"overlay2","features":{"containerd-snapshotter":false}}' \
  >/etc/docker/daemon.json
if systemctl enable --now containerd docker >/dev/null 2>&1 \
  && systemctl restart docker \
  && docker info >/var/tmp/ucloud-probe-docker-info.log 2>&1; then
  pass docker_daemon
else
  fail docker_daemon "see /var/tmp/ucloud-probe-docker-info.log"
fi

docker_driver="$(docker info --format '{{.Driver}}' 2>/dev/null || true)"
if [[ "$docker_driver" == overlay2 ]]; then
  pass docker_overlay2
else
  fail docker_overlay2 "driver=$docker_driver"
fi

if docker run --rm alpine:3.20 sh -c 'test -r /proc/self/status && printf docker-ok' \
    | grep -qx docker-ok; then
  pass docker_container
else
  fail docker_container
fi

if docker run -d --name ucloud-port-probe -p 127.0.0.1:18080:80 \
    alpine:3.20 sh -c \
    'mkdir -p /www && printf port-ok >/www/index.html && exec busybox httpd -f -p 80 -h /www' \
    >/var/tmp/ucloud-probe-port-id 2>/var/tmp/ucloud-probe-port.log \
  && curl -fsS http://127.0.0.1:18080 | grep -qx port-ok; then
  pass docker_host_port_publish
else
  warn docker_host_port_publish \
    "unsupported on this kernel; direct sandbox networking is tested separately"
fi
docker rm -f ucloud-port-probe >/dev/null 2>&1 || true

if printf 'FROM alpine:3.20\nRUN printf build-ok >/result\n' \
    | docker buildx build --load -t ucloud-build-probe - >/var/tmp/ucloud-probe-build.log 2>&1 \
  && [[ "$(docker run --rm ucloud-build-probe cat /result)" == build-ok ]]; then
  pass docker_buildx
else
  fail docker_buildx "see /var/tmp/ucloud-probe-build.log"
fi

if docker run -d --privileged --name ucloud-dind-probe docker:28-dind \
    >/var/tmp/ucloud-probe-dind-id 2>/var/tmp/ucloud-probe-dind.log; then
  dind_ready=0
  for _ in $(seq 1 40); do
    if docker exec ucloud-dind-probe docker info >/dev/null 2>&1; then
      dind_ready=1
      break
    fi
    sleep 1
  done
  if [[ "$dind_ready" == 1 ]]; then
    pass docker_in_docker
  else
    fail docker_in_docker "daemon did not become ready"
  fi
else
  fail docker_in_docker "container did not start"
fi
docker rm -f ucloud-dind-probe >/dev/null 2>&1 || true

section gvisor
gvisor_url='https://storage.googleapis.com/gvisor/releases/release/20260721.0/x86_64'
if curl -fsSLo "$probe_root/gvisor.tar.bz2" "$gvisor_url/gvisor.tar.bz2" \
  && curl -fsSLo "$probe_root/gvisor.tar.bz2.sha512" "$gvisor_url/gvisor.tar.bz2.sha512" \
  && (cd "$probe_root" && sha512sum -c gvisor.tar.bz2.sha512 >/dev/null) \
  && tar -xjf "$probe_root/gvisor.tar.bz2" -C /usr/local/bin \
  && /usr/local/bin/runsc --version >/var/tmp/ucloud-probe-runsc-version.log 2>&1; then
  pass gvisor_pinned_release
else
  fail gvisor_pinned_release "see /var/tmp/ucloud-probe-runsc-version.log"
fi

if /usr/local/bin/runsc install --runtime runsc-systrap -- --platform=systrap \
    >/var/tmp/ucloud-probe-runsc-install.log 2>&1 \
  && systemctl restart docker \
  && docker run --rm --runtime=runsc-systrap alpine:3.20 \
    sh -c 'uname -r; test -r /proc/self/status' \
    >/var/tmp/ucloud-probe-runsc-container.log 2>&1; then
  pass gvisor_systrap_container "$(head -n 1 /var/tmp/ucloud-probe-runsc-container.log)"
else
  fail gvisor_systrap_container "see /var/tmp/ucloud-probe-runsc-container.log"
fi

section result
printf 'SUMMARY\tfailures\t%d\n' "$failures"
if [[ "$failures" -gt 0 ]]; then
  exit 1
fi
exit "$((failures > 0))"
